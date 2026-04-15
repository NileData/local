import { existsSync, copyFileSync, mkdirSync } from 'fs';
import { join, basename } from 'path';
import { randomUUID } from 'crypto';
import type { RequestHandler } from 'express';
import type { SparkEngine } from '../../engines/spark-engine.js';
import type { SQLiteService } from '../../services/sqlite-service.js';

export function createLocalFileImportHandler(sparkEngine: SparkEngine, sqliteService: SQLiteService): RequestHandler {
  return async (req, res) => {
    const body = req.body as Record<string, unknown> | undefined;
    const filePath = body?.filePath as string | undefined;
    const targetDatabase = body?.targetDatabase as string | undefined;
    const targetTable = body?.targetTable as string | undefined;

    if (!filePath || !targetDatabase || !targetTable) {
      res.status(400).json({ error: 'VALIDATION_ERROR', message: '"filePath", "targetDatabase", and "targetTable" are required.' });
      return;
    }
    if (!existsSync(filePath)) {
      res.status(400).json({ error: 'FILE_NOT_FOUND', message: `File not found: ${filePath}` });
      return;
    }

    // Ensure file is accessible from Spark container
    const containerPath = copyToWarehouseStaging(filePath, sparkEngine);

    const queryExecutionId = `qe-${Date.now()}-${randomUUID().slice(0, 8)}`;
    const now = new Date().toISOString();

    sqliteService.insertJob({
      jobId: queryExecutionId,
      type: 'import',
      status: 'QUEUED',
      engine: 'spark',
      inputJson: JSON.stringify({ filePath, targetDatabase, targetTable }),
      submittedAt: now,
    });

    // Await the import so the table exists in Spark before responding
    try {
      await runImport(queryExecutionId, containerPath, filePath, targetDatabase, targetTable, sparkEngine, sqliteService);
      res.json({
        success: true,
        queryExecutionId,
        tableId: `${targetDatabase}.${targetTable}`,
        database: targetDatabase,
        tableName: targetTable,
      });
    } catch (err) {
      res.status(500).json({
        error: 'IMPORT_FAILED',
        message: err instanceof Error ? err.message : String(err),
      });
    }
  };
}

async function runImport(
  queryExecutionId: string,
  containerPath: string,
  originalFilePath: string,
  targetDatabase: string,
  targetTable: string,
  sparkEngine: SparkEngine,
  sqliteService: SQLiteService,
): Promise<void> {
  try {
    sqliteService.updateJob(queryExecutionId, { status: 'RUNNING' });

    const ext = originalFilePath.toLowerCase().split('.').pop() ?? '';
    const sparkFormat = ext === 'parquet' ? 'parquet' :
                        ext === 'json'    ? 'json' :
                                            'csv';

    // Ensure namespace exists (may fail if name collides with catalog)
    try {
      await sparkEngine.executeDDL(`CREATE NAMESPACE IF NOT EXISTS ${targetDatabase}`);
    } catch {
      // Namespace likely already exists
    }

    // Create Iceberg table from file via Spark CTAS
    if (sparkFormat === 'csv') {
      // CSV needs header and inferSchema options via temp view
      const tempView = `_file_import_${Date.now()}`;
      await sparkEngine.executeDDL(
        `CREATE TEMPORARY VIEW ${tempView} USING csv OPTIONS (path '${containerPath}', header 'true', inferSchema 'true')`
      );

      // Drop trailing-delimiter ghost columns (_c0, _c1, …) that are entirely null
      const schemaResult = await sparkEngine.executeSQL(`SELECT * FROM ${tempView} LIMIT 0`);
      const ghostCols = schemaResult.columns
        .filter(c => /^_c\d+$/.test(c.name))
        .map(c => c.name);
      let selectCols = '*';
      if (ghostCols.length > 0) {
        // Check if ghost columns are all null
        const nullChecks = ghostCols.map(c => `COUNT(${c}) AS cnt_${c}`).join(', ');
        const nullResult = await sparkEngine.executeSQL(`SELECT ${nullChecks} FROM ${tempView}`);
        const allNullGhosts = ghostCols.filter(c => {
          const count = Number(nullResult.rows[0]?.[`cnt_${c}`] ?? 0);
          return count === 0;
        });
        if (allNullGhosts.length > 0) {
          const keepCols = schemaResult.columns
            .filter(c => !allNullGhosts.includes(c.name))
            .map(c => c.name);
          if (keepCols.length > 0) {
            selectCols = keepCols.map(c => `\`${c}\``).join(', ');
          }
        }
      }

      await sparkEngine.executeDDL(
        `CREATE TABLE ${targetDatabase}.${targetTable} USING ICEBERG AS SELECT ${selectCols} FROM ${tempView}`
      );
      try { await sparkEngine.executeDDL(`DROP VIEW IF EXISTS ${tempView}`); } catch { /* non-critical */ }
    } else {
      await sparkEngine.executeDDL(
        `CREATE TABLE ${targetDatabase}.${targetTable} USING ICEBERG AS SELECT * FROM ${sparkFormat}.\`${containerPath}\``
      );
    }

    // Get schema and row count from Spark
    const descResult = await sparkEngine.executeSQL(`DESCRIBE TABLE ${targetDatabase}.${targetTable}`);
    const columns = descResult.rows.map(row => ({
      name: String(row['col_name']),
      dataType: String(row['data_type']),
      nullable: true,
    }));

    const countResult = await sparkEngine.executeSQL(`SELECT COUNT(*) AS row_count FROM ${targetDatabase}.${targetTable}`);
    const rowCount = Number(countResult.rows[0]?.['row_count'] ?? 0);

    sqliteService.insertDatabase(targetDatabase);
    sqliteService.insertTable(targetDatabase, targetTable, { schemaJson: JSON.stringify(columns), sourcePath: originalFilePath, rowCount });
    sqliteService.insertEvent('table_created', `Imported ${targetDatabase}.${targetTable} from ${originalFilePath}`, { rowCount });

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

/**
 * Copy a host file into the warehouse staging directory so the Spark container can access it.
 * Returns the container-side path.
 */
function copyToWarehouseStaging(hostPath: string, sparkEngine: SparkEngine): string {
  const warehouseDir = sparkEngine.getWarehouseDir().replace(/\\/g, '/');
  const normalizedHost = hostPath.replace(/\\/g, '/');

  // If already inside warehouse, just map path
  if (normalizedHost.startsWith(warehouseDir)) {
    const relative = normalizedHost.slice(warehouseDir.length);
    return `/warehouse${relative}`;
  }

  // Copy to staging
  const stagingDir = join(sparkEngine.getWarehouseDir(), '_staging');
  mkdirSync(stagingDir, { recursive: true });
  const destFile = join(stagingDir, basename(hostPath));
  copyFileSync(hostPath, destFile);
  return `/warehouse/_staging/${basename(hostPath)}`;
}
