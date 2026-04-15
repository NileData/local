import { writeFileSync, mkdirSync } from 'fs';
import { join } from 'path';
import { randomUUID } from 'crypto';
import type { RequestHandler } from 'express';
import type { SparkEngine } from '../../engines/spark-engine.js';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { LocalConfig } from '../../config/environment.js';

/**
 * Paste import handler -- accepts the cloud format (ImportFromPasteRequest):
 *   { database, tableName, schema: Column[], rows: unknown[][], description? }
 * Also supports a simpler legacy format:
 *   { content: string (CSV), targetDatabase, targetTable }
 */
export function createImportPasteHandler(config: LocalConfig, sparkEngine: SparkEngine, sqliteService: SQLiteService): RequestHandler {
  return async (req, res) => {
    const body = req.body as Record<string, unknown> | undefined;
    if (!body) {
      res.status(400).json({ error: 'VALIDATION_ERROR', message: 'Request body is required.' });
      return;
    }

    let csvContent: string;
    let targetDatabase: string;
    let targetTable: string;
    let description: string | undefined;

    if (body.rows && body.schema && body.tableName) {
      // Cloud format: ImportFromPasteRequest
      targetDatabase = (body.database as string) || 'local';
      targetTable = body.tableName as string;
      description = body.description as string | undefined;
      const schema = body.schema as Array<{ name: string; dataType?: string }>;
      const rows = body.rows as unknown[][];

      // Convert schema + rows to CSV
      const header = schema.map(c => c.name).join(',');
      const csvRows = rows.map(row =>
        row.map(cell => {
          if (cell === null || cell === undefined) return '';
          const str = String(cell);
          // Quote if contains comma, newline, or quote
          if (str.includes(',') || str.includes('\n') || str.includes('"')) {
            return `"${str.replace(/"/g, '""')}"`;
          }
          return str;
        }).join(',')
      );
      csvContent = [header, ...csvRows].join('\n');
    } else if (body.content && body.targetDatabase && body.targetTable) {
      // Legacy format
      csvContent = body.content as string;
      targetDatabase = body.targetDatabase as string;
      targetTable = body.targetTable as string;
      description = body.description as string | undefined;
    } else {
      res.status(400).json({
        error: 'VALIDATION_ERROR',
        message: 'Provide either {database, tableName, schema, rows} or {content, targetDatabase, targetTable}.',
      });
      return;
    }

    // Write CSV to a staging file inside the warehouse so Spark container can access it
    const stagingDir = join(sparkEngine.getWarehouseDir(), '_staging');
    mkdirSync(stagingDir, { recursive: true });
    const tmpFileName = `paste-${randomUUID()}.csv`;
    const tmpFile = join(stagingDir, tmpFileName);
    writeFileSync(tmpFile, csvContent, 'utf-8');
    const containerPath = `/warehouse/_staging/${tmpFileName}`;

    const queryExecutionId = `qe-${Date.now()}-${randomUUID().slice(0, 8)}`;
    const now = new Date().toISOString();

    sqliteService.insertJob({
      jobId: queryExecutionId,
      type: 'import',
      status: 'QUEUED',
      engine: 'spark',
      inputJson: JSON.stringify({ filePath: tmpFile, targetDatabase, targetTable }),
      submittedAt: now,
    });

    // Await the import so the table exists in Spark before responding
    try {
      await runPasteImport(queryExecutionId, containerPath, targetDatabase, targetTable, sparkEngine, sqliteService, description);
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

async function runPasteImport(
  queryExecutionId: string,
  containerCsvPath: string,
  targetDatabase: string,
  targetTable: string,
  sparkEngine: SparkEngine,
  sqliteService: SQLiteService,
  description?: string,
): Promise<void> {
  try {
    sqliteService.updateJob(queryExecutionId, { status: 'RUNNING' });

    // Ensure namespace exists (may fail if name collides with catalog, e.g. "local")
    try {
      await sparkEngine.executeDDL(`CREATE NAMESPACE IF NOT EXISTS ${targetDatabase}`);
    } catch {
      // Namespace likely already exists (seeded on startup)
    }

    // Create Iceberg table from CSV via Spark CTAS
    // Use CREATE TEMPORARY VIEW with OPTIONS to handle CSV with header, then CTAS from that view
    const tempView = `_paste_import_${Date.now()}`;
    await sparkEngine.executeDDL(
      `CREATE TEMPORARY VIEW ${tempView} USING csv OPTIONS (path '${containerCsvPath}', header 'true', inferSchema 'true')`
    );
    await sparkEngine.executeDDL(
      `CREATE TABLE ${targetDatabase}.${targetTable} USING ICEBERG AS SELECT * FROM ${tempView}`
    );
    // Clean up temp view
    try { await sparkEngine.executeDDL(`DROP VIEW IF EXISTS ${tempView}`); } catch { /* non-critical */ }

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
    sqliteService.insertTable(targetDatabase, targetTable, { description, schemaJson: JSON.stringify(columns), rowCount });
    sqliteService.insertEvent('table_created', `Pasted data imported to ${targetDatabase}.${targetTable}`, { rowCount });
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
