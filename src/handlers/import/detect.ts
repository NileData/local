import { existsSync, copyFileSync, mkdirSync } from 'fs';
import { join, basename } from 'path';
import type { RequestHandler } from 'express';
import type { SparkEngine } from '../../engines/spark-engine.js';

export function createImportDetectHandler(sparkEngine: SparkEngine): RequestHandler {
  return async (req, res) => {
    const body = req.body as Record<string, unknown> | undefined;
    // Accept filePath (local file) or s3Location (S3 path)
    const filePath = (body?.filePath ?? body?.s3Location) as string | undefined;

    if (!filePath) {
      res.status(400).json({ error: 'VALIDATION_ERROR', message: '"filePath" or "s3Location" is required in local mode.' });
      return;
    }

    const isS3 = filePath.startsWith('s3://') || filePath.startsWith('s3a://');

    if (!isS3 && !existsSync(filePath)) {
      res.status(400).json({ error: 'FILE_NOT_FOUND', message: `File not found: ${filePath}` });
      return;
    }

    // For local files, copy to staging; for S3, convert to s3a://
    const readPath = isS3
      ? filePath.replace(/^s3:\/\//, 's3a://')
      : copyToWarehouseStaging(filePath, sparkEngine);

    // Detect format from explicit field, extension, or default
    const explicitFormat = body?.format as string | undefined;
    const sparkFormat = explicitFormat
      ? normalizeFormat(explicitFormat)
      : detectFormatFromPath(filePath);

    try {
      let descResult;
      let countResult;

      let sampleResult;

      if (sparkFormat === 'csv') {
        const tempView = `_detect_${Date.now()}`;
        await sparkEngine.executeDDL(
          `CREATE TEMPORARY VIEW ${tempView} USING csv OPTIONS (path '${readPath}', header 'true', inferSchema 'true')`
        );
        descResult = await sparkEngine.executeSQL(`SELECT * FROM ${tempView} LIMIT 0`);

        // Drop trailing-delimiter ghost columns (_c0, _c1, …) that are all null
        const ghostCols = descResult.columns
          .filter(c => /^_c\d+$/.test(c.name))
          .map(c => c.name);
        if (ghostCols.length > 0) {
          const nullChecks = ghostCols.map(c => `COUNT(${c}) AS cnt_${c}`).join(', ');
          const nullResult = await sparkEngine.executeSQL(`SELECT ${nullChecks} FROM ${tempView}`);
          const allNullGhosts = ghostCols.filter(c => {
            const count = Number(nullResult.rows[0]?.[`cnt_${c}`] ?? 0);
            return count === 0;
          });
          if (allNullGhosts.length > 0) {
            descResult = {
              ...descResult,
              columns: descResult.columns.filter(c => !allNullGhosts.includes(c.name)),
            };
          }
        }

        countResult = await sparkEngine.executeSQL(`SELECT COUNT(*) AS row_count FROM ${tempView}`);
        sampleResult = await sparkEngine.executeSQL(`SELECT * FROM ${tempView} LIMIT 50`);
        try { await sparkEngine.executeDDL(`DROP VIEW IF EXISTS ${tempView}`); } catch { /* non-critical */ }
      } else {
        descResult = await sparkEngine.executeSQL(
          `SELECT * FROM ${sparkFormat}.\`${readPath}\` LIMIT 0`
        );
        countResult = await sparkEngine.executeSQL(
          `SELECT COUNT(*) AS row_count FROM ${sparkFormat}.\`${readPath}\``
        );
        sampleResult = await sparkEngine.executeSQL(
          `SELECT * FROM ${sparkFormat}.\`${readPath}\` LIMIT 50`
        );
      }

      const columns = descResult.columns.map(col => ({
        name: col.name,
        type: col.type,
        nullable: true,
      }));
      const rowCount = Number(countResult.rows[0]?.['row_count'] ?? 0);

      // Convert sample rows to array-of-arrays format (matching cloud detect response)
      const sampleData = (sampleResult?.rows || []).map(row =>
        columns.map(col => row[col.name] ?? null)
      );

      res.json({ columns, rowCount, sampleData, fileSizeBytes: 0 });
    } catch (err) {
      res.status(400).json({ error: 'DETECTION_FAILED', message: err instanceof Error ? err.message : String(err) });
    }
  };
}

function detectFormatFromPath(path: string): string {
  const ext = path.toLowerCase().split('.').pop() ?? '';
  if (ext === 'parquet') return 'parquet';
  if (ext === 'json') return 'json';
  if (ext === 'orc') return 'orc';
  if (ext === 'avro') return 'avro';
  return 'csv';
}

function normalizeFormat(format: string): string {
  const f = format.toLowerCase();
  if (f === 'csv') return 'csv';
  if (f === 'json') return 'json';
  if (f === 'orc') return 'orc';
  if (f === 'avro') return 'avro';
  return 'parquet';
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
