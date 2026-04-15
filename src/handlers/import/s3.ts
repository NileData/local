import { randomUUID } from 'crypto';
import type { RequestHandler } from 'express';
import type { SparkEngine } from '../../engines/spark-engine.js';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { ImportFromS3Request } from '../../types/types.js';
import { configureSparkS3Credentials } from './s3-credentials.js';

/**
 * S3 import handler (local mode) -- reads data from S3 via Spark's s3a:// support
 * and creates a local Iceberg table. Synchronous (no Step Functions).
 *
 * Requires valid AWS credentials on the host (passed to Spark container
 * via buildCloudCredentialFlags).
 */
export function createImportS3Handler(sparkEngine: SparkEngine, sqliteService: SQLiteService): RequestHandler {
  return async (req, res) => {
    const body = req.body as ImportFromS3Request | undefined;
    if (!body?.database || !body?.tableName || !body?.s3Location) {
      res.status(400).json({ error: 'VALIDATION_ERROR', message: '"database", "tableName", and "s3Location" are required.' });
      return;
    }

    const { database, tableName, s3Location, description } = body;
    const format = body.format ?? 'parquet';

    // Convert s3:// to s3a:// for Hadoop FileSystem
    const s3aPath = s3Location.replace(/^s3:\/\//, 's3a://');

    const queryExecutionId = `qe-${Date.now()}-${randomUUID().slice(0, 8)}`;
    const now = new Date().toISOString();

    sqliteService.insertJob({
      jobId: queryExecutionId,
      type: 'import',
      status: 'QUEUED',
      engine: 'spark',
      inputJson: JSON.stringify({ s3Location, database, tableName, format }),
      submittedAt: now,
    });

    try {
      await runS3Import(queryExecutionId, s3aPath, format, database, tableName, sparkEngine, sqliteService, description, body.csvOptions);
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

async function runS3Import(
  queryExecutionId: string,
  s3aPath: string,
  format: string,
  targetDatabase: string,
  targetTable: string,
  sparkEngine: SparkEngine,
  sqliteService: SQLiteService,
  description?: string,
  csvOptions?: { delimiter?: string; hasHeader?: boolean },
): Promise<void> {
  try {
    sqliteService.updateJob(queryExecutionId, { status: 'RUNNING' });

    // Resolve AWS credentials from the host's credential chain (SSO, profiles,
    // env vars, etc.) and configure Spark's Hadoop S3 provider at query time.
    // This only affects S3 imports -- no global side effects.
    await configureSparkS3Credentials(sparkEngine);

    // Ensure namespace exists
    try {
      await sparkEngine.executeDDL(`CREATE NAMESPACE IF NOT EXISTS ${targetDatabase}`);
    } catch {
      // Namespace likely already exists
    }

    const sparkFormat = normalizeFormat(format);
    const tempView = `_s3_import_${Date.now()}`;

    if (sparkFormat === 'csv') {
      const header = csvOptions?.hasHeader !== false ? 'true' : 'false';
      const delimiter = csvOptions?.delimiter ?? ',';
      await sparkEngine.executeDDL(
        `CREATE TEMPORARY VIEW ${tempView} USING csv OPTIONS (path '${s3aPath}', header '${header}', inferSchema 'true', delimiter '${delimiter}')`
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
      sourcePath: s3aPath.replace(/^s3a:\/\//, 's3://'),
      rowCount,
    });
    sqliteService.insertEvent('table_created', `S3 import to ${targetDatabase}.${targetTable}`, { rowCount, source: s3aPath });
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

function normalizeFormat(format: string): string {
  const f = format.toLowerCase();
  if (f === 'csv') return 'csv';
  if (f === 'json') return 'json';
  if (f === 'orc') return 'orc';
  if (f === 'avro') return 'avro';
  return 'parquet';
}
