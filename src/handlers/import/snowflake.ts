import { randomUUID } from 'crypto';
import type { RequestHandler } from 'express';
import type { SparkEngine } from '../../engines/spark-engine.js';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { ImportFromSnowflakeRequest } from '../../types/types.js';

/**
 * Snowflake import handler (local mode) -- reads data from Snowflake via
 * Spark-Snowflake connector and creates a local Iceberg table. Synchronous.
 */
export function createImportSnowflakeHandler(sparkEngine: SparkEngine, sqliteService: SQLiteService): RequestHandler {
  return async (req, res) => {
    const body = req.body as ImportFromSnowflakeRequest | undefined;
    if (!body?.database || !body?.tableName || !body?.connectionId || !body?.sourceTable) {
      res.status(400).json({ error: 'VALIDATION_ERROR', message: '"database", "tableName", "connectionId", and "sourceTable" are required.' });
      return;
    }

    const { database, tableName, connectionId, sourceTable, description } = body;
    const sourceSchema = body.sourceSchema ?? 'PUBLIC';

    // Look up connection from SQLite
    const conn = sqliteService.getConnection(connectionId);
    if (!conn) {
      res.status(404).json({ error: 'CONNECTION_NOT_FOUND', message: `Connection ${connectionId} not found.` });
      return;
    }

    let config: Record<string, string>;
    try {
      config = JSON.parse(conn.configJson) as Record<string, string>;
    } catch {
      res.status(500).json({ error: 'INVALID_CONNECTION', message: 'Connection configuration is corrupted.' });
      return;
    }

    const queryExecutionId = `qe-${Date.now()}-${randomUUID().slice(0, 8)}`;
    const now = new Date().toISOString();

    sqliteService.insertJob({
      jobId: queryExecutionId,
      type: 'import',
      status: 'QUEUED',
      engine: 'spark',
      inputJson: JSON.stringify({ connectionId, sourceSchema, sourceTable, database, tableName }),
      submittedAt: now,
    });

    try {
      await runSnowflakeImport(queryExecutionId, config, sourceSchema, sourceTable, database, tableName, sparkEngine, sqliteService, description);
      res.json({
        success: true,
        queryExecutionId,
        tableId: `${database}.${tableName}`,
        database,
        tableName,
      });
    } catch (err) {
      res.status(500).json({
        error: 'IMPORT_FAILED',
        message: err instanceof Error ? err.message : String(err),
      });
    }
  };
}

async function runSnowflakeImport(
  queryExecutionId: string,
  config: Record<string, string>,
  sourceSchema: string,
  sourceTable: string,
  targetDatabase: string,
  targetTable: string,
  sparkEngine: SparkEngine,
  sqliteService: SQLiteService,
  description?: string,
): Promise<void> {
  try {
    sqliteService.updateJob(queryExecutionId, { status: 'RUNNING' });

    // Ensure namespace exists
    try {
      await sparkEngine.executeDDL(`CREATE NAMESPACE IF NOT EXISTS ${targetDatabase}`);
    } catch {
      // Namespace likely already exists
    }

    const account = config['account'] ?? '';
    const user = config['username'] ?? '';
    const password = config['password'] ?? '';
    const sfDatabase = config['database'] ?? '';
    const warehouse = config['warehouse'] ?? '';
    const role = config['role'] ?? '';
    const tempView = `_sf_import_${Date.now()}`;

    // Use Spark-Snowflake connector via PySpark for reliable type mapping
    const pyCode = `
df = spark.read.format("snowflake") \\
    .option("sfURL", "${escapePy(account)}.snowflakecomputing.com") \\
    .option("sfUser", "${escapePy(user)}") \\
    .option("sfPassword", "${escapePy(password)}") \\
    .option("sfDatabase", "${escapePy(sfDatabase)}") \\
    .option("sfSchema", "${escapePy(sourceSchema)}") \\
    .option("sfWarehouse", "${escapePy(warehouse)}") \\
    .option("sfRole", "${escapePy(role)}") \\
    .option("dbtable", "${escapePy(sourceTable)}") \\
    .load()
df.createOrReplaceTempView("${tempView}")
`;
    await sparkEngine.executePython(pyCode);

    await sparkEngine.executeDDL(
      `CREATE TABLE ${targetDatabase}.${targetTable} USING ICEBERG AS SELECT * FROM ${tempView}`
    );
    try { await sparkEngine.executeDDL(`DROP VIEW IF EXISTS ${tempView}`); } catch { /* non-critical */ }

    // Get schema and row count
    const descResult = await sparkEngine.executeSQL(`DESCRIBE TABLE ${targetDatabase}.${targetTable}`);
    const columns = descResult.rows.map(row => ({
      name: String(row['col_name']),
      dataType: String(row['data_type']),
      nullable: true,
    }));

    const countResult = await sparkEngine.executeSQL(`SELECT COUNT(*) AS row_count FROM ${targetDatabase}.${targetTable}`);
    const rowCount = Number(countResult.rows[0]?.['row_count'] ?? 0);

    sqliteService.insertDatabase(targetDatabase);
    sqliteService.insertTable(targetDatabase, targetTable, {
      description,
      schemaJson: JSON.stringify(columns),
      sourcePath: `snowflake://${account}/${sfDatabase}/${sourceSchema}.${sourceTable}`,
      rowCount,
    });
    sqliteService.insertEvent('table_created', `Snowflake import to ${targetDatabase}.${targetTable}`, { rowCount, source: `${sourceSchema}.${sourceTable}` });
    sqliteService.updateJob(queryExecutionId, {
      status: 'COMPLETED',
      resultJson: JSON.stringify({ rowCount, database: targetDatabase, table: targetTable }),
      completedAt: new Date().toISOString(),
    });
  } catch (err) {
    sqliteService.updateJob(queryExecutionId, {
      status: 'FAILED',
      errorMessage: err instanceof Error ? err.message : String(err),
      completedAt: new Date().toISOString(),
    });
    throw err;
  }
}

/** Escape a string for use in a Python string literal (single-line, double-quoted). */
function escapePy(s: string): string {
  return s.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n');
}
