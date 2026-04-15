import type { RequestHandler } from 'express';
import type { SparkEngine } from '../../engines/spark-engine.js';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { ListSnapshotsResponse, SnapshotInfo } from '../../types/types.js';

/**
 * ListSnapshots handler for local mode.
 * Queries the Iceberg snapshots metadata table via direct SQL.
 * Uses executeSQL instead of executePython — SparkJsonEncoder handles datetime serialization.
 */
export function createListSnapshotsHandler(sparkEngine: SparkEngine, sqliteService: SQLiteService): RequestHandler {
  return async (req, res) => {
    const { database, table } = req.params as { database: string; table: string };

    // Verify table exists in catalog
    const row = sqliteService.getTable(database, table);
    if (!row) {
      res.status(404).json({ error: 'NOT_FOUND', message: `Table '${database}.${table}' not found.` });
      return;
    }

    const limit = Math.min(Number(req.query['limit']) || 100, 1000);

    try {
      const sql = `SELECT snapshot_id, committed_at, operation, summary FROM ${database}.${table}.snapshots ORDER BY committed_at DESC LIMIT ${limit}`;
      const result = await sparkEngine.executeSQL(sql);

      let snapshots: SnapshotInfo[] = [];
      if (result.rows && result.rows.length > 0) {
        const mapped = result.rows.map((row) => {
          // SparkJsonEncoder converts datetime to ISO string
          const committedAt = String(row['committed_at'] ?? '');
          const isoTimestamp = committedAt.includes('T') ? committedAt : committedAt.replace(' ', 'T');
          const withZ = isoTimestamp.endsWith('Z') ? isoTimestamp : isoTimestamp + 'Z';

          // summary comes as a Map/dict from Spark
          const rawSummary = row['summary'];
          const summary: Record<string, unknown> = typeof rawSummary === 'object' && rawSummary !== null
            ? (rawSummary as Record<string, unknown>)
            : {};

          return {
            snapshotId: String(row['snapshot_id'] ?? ''),
            committedAt: withZ,
            operation: String(row['operation'] ?? ''),
            summary,
          };
        });

        // Add parentSnapshotId chain (each snapshot's parent is the next one in time-descending order)
        snapshots = mapped.map((s, i) => ({
          ...s,
          parentSnapshotId: i < mapped.length - 1 ? mapped[i + 1].snapshotId : undefined,
        }));
      }

      const response: ListSnapshotsResponse = { snapshots };
      res.json(response);
    } catch (err) {
      console.warn('[list-snapshots] Failed to query Iceberg snapshots:', err instanceof Error ? err.message : err);
      const response: ListSnapshotsResponse = { snapshots: [] };
      res.json(response);
    }
  };
}
