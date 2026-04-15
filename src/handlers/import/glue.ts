import { randomUUID } from 'crypto';
import type { RequestHandler } from 'express';
import type { SparkEngine } from '../../engines/spark-engine.js';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { ImportFromGlueRequest } from '../../types/types.js';
import { configureSparkS3Credentials } from './s3-credentials.js';

/**
 * Glue import handler (local mode) -- uses AWS SDK to look up the Glue table's
 * underlying S3 location, then reads data via Spark's s3a:// and creates a
 * local Iceberg table. Synchronous.
 *
 * This is the "simple S3-location" approach: no second Spark catalog needed.
 */
export function createImportGlueHandler(sparkEngine: SparkEngine, sqliteService: SQLiteService): RequestHandler {
  return async (req, res) => {
    const body = req.body as ImportFromGlueRequest | undefined;
    if (!body?.database || !body?.tableName || !body?.sourceDatabase || !body?.sourceTable) {
      res.status(400).json({ error: 'VALIDATION_ERROR', message: '"database", "tableName", "sourceDatabase", and "sourceTable" are required.' });
      return;
    }

    const { database, tableName, sourceDatabase, sourceTable, sourceAccountId, description } = body;

    const queryExecutionId = `qe-${Date.now()}-${randomUUID().slice(0, 8)}`;
    const now = new Date().toISOString();

    sqliteService.insertJob({
      jobId: queryExecutionId,
      type: 'import',
      status: 'QUEUED',
      engine: 'spark',
      inputJson: JSON.stringify({ sourceDatabase, sourceTable, sourceAccountId, database, tableName }),
      submittedAt: now,
    });

    try {
      await runGlueImport(queryExecutionId, sourceDatabase, sourceTable, sourceAccountId, database, tableName, sparkEngine, sqliteService, description);
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

async function runGlueImport(
  queryExecutionId: string,
  sourceDatabase: string,
  sourceTable: string,
  sourceAccountId: string | undefined,
  targetDatabase: string,
  targetTable: string,
  sparkEngine: SparkEngine,
  sqliteService: SQLiteService,
  description?: string,
): Promise<void> {
  try {
    sqliteService.updateJob(queryExecutionId, { status: 'RUNNING' });

    // Resolve AWS credentials for both the Glue SDK call and Spark S3 reads
    await configureSparkS3Credentials(sparkEngine);

    // Use AWS SDK to look up the Glue table's S3 location
    const { GlueClient, GetTableCommand } = await import('@aws-sdk/client-glue');
    const glueClient = new GlueClient({});

    const glueResponse = await glueClient.send(new GetTableCommand({
      CatalogId: sourceAccountId,
      DatabaseName: sourceDatabase,
      Name: sourceTable,
    }));

    const storageDescriptor = glueResponse.Table?.StorageDescriptor;
    if (!storageDescriptor?.Location) {
      throw new Error(`Glue table ${sourceDatabase}.${sourceTable} has no S3 location.`);
    }

    const s3Location = storageDescriptor.Location;
    const s3aPath = s3Location.replace(/^s3:\/\//, 's3a://');

    // Detect format from Glue table metadata
    const inputFormat = storageDescriptor.InputFormat ?? '';
    const serdeLib = storageDescriptor.SerdeInfo?.SerializationLibrary ?? '';
    const sparkFormat = detectFormatFromGlue(inputFormat, serdeLib);

    // Ensure namespace exists
    try {
      await sparkEngine.executeDDL(`CREATE NAMESPACE IF NOT EXISTS ${targetDatabase}`);
    } catch {
      // Namespace likely already exists
    }

    // Create Iceberg table from S3 data
    if (sparkFormat === 'csv') {
      const tempView = `_glue_import_${Date.now()}`;
      await sparkEngine.executeDDL(
        `CREATE TEMPORARY VIEW ${tempView} USING csv OPTIONS (path '${s3aPath}', header 'true', inferSchema 'true')`
      );
      await sparkEngine.executeDDL(
        `CREATE TABLE ${targetDatabase}.${targetTable} USING ICEBERG AS SELECT * FROM ${tempView}`
      );
      try { await sparkEngine.executeDDL(`DROP VIEW IF EXISTS ${tempView}`); } catch { /* non-critical */ }
    } else {
      await sparkEngine.executeDDL(
        `CREATE TABLE ${targetDatabase}.${targetTable} USING ICEBERG AS SELECT * FROM ${sparkFormat}.\`${s3aPath}\``
      );
    }

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
      sourcePath: s3Location,
      rowCount,
    });
    sqliteService.insertEvent('table_created', `Glue import to ${targetDatabase}.${targetTable}`, {
      rowCount,
      source: `${sourceDatabase}.${sourceTable}`,
      glueLocation: s3Location,
    });
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

/** Map Glue InputFormat / SerDe to a Spark format name. */
function detectFormatFromGlue(inputFormat: string, serdeLib: string): string {
  const combined = `${inputFormat} ${serdeLib}`.toLowerCase();
  if (combined.includes('parquet')) return 'parquet';
  if (combined.includes('orc')) return 'orc';
  if (combined.includes('json')) return 'json';
  if (combined.includes('avro')) return 'avro';
  if (combined.includes('csv') || combined.includes('opencsv') || combined.includes('lazysimple')) return 'csv';
  // Default to parquet for unknown formats
  return 'parquet';
}
