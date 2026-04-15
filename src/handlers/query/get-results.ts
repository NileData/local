import { readFileSync } from 'fs';
import type { RequestHandler } from 'express';
import type { QueryWorkerPool } from '../../services/query-worker-pool.js';
import type { SQLiteService } from '../../services/sqlite-service.js';

interface StoredResult {
  columns: Array<{ name: string; type: string }>;
  rows: Record<string, unknown>[];
  rowCount: number;
}

export function createQueryResultsHandler(
  queryWorkerPool: QueryWorkerPool,
  sqliteService: SQLiteService,
): RequestHandler {
  return (req, res) => {
    const { queryExecutionId } = req.params;
    if (!queryExecutionId) {
      res.status(400).json({
        error: 'VALIDATION_ERROR',
        message: 'queryExecutionId parameter is required.',
      });
      return;
    }

    const job = sqliteService.getJob(queryExecutionId);
    if (!job) {
      res.status(404).json({
        error: 'NOT_FOUND',
        message: `Query execution ${queryExecutionId} not found.`,
      });
      return;
    }

    const runtimeState = queryWorkerPool.getRuntimeState(queryExecutionId);

    // If still running or queued, return status without rows
    if (job.status === "QUEUED" || job.status === "RUNNING") {
      res.status(200).json({
        queryExecutionId: job.jobId,
        status: job.status,
        results: {},
        ...(runtimeState?.queuePosition !== undefined ? { queuePosition: runtimeState.queuePosition } : {}),
        ...(runtimeState?.queuedCountAhead !== undefined ? { queuedCountAhead: runtimeState.queuedCountAhead } : {}),
        ...(runtimeState?.workerId ? { workerId: runtimeState.workerId } : {}),
      });
      return;
    }

    // If failed or cancelled
    if (job.status === "FAILED" || job.status === "CANCELLED") {
      res.status(200).json({
        queryExecutionId: job.jobId,
        status: job.status,
        results: {},
        error: job.errorMessage ?? undefined,
        ...(runtimeState?.workerId ? { workerId: runtimeState.workerId } : {}),
      });
      return;
    }

    // COMPLETED -- load results from disk (like cloud reads from S3)
    let resultData: StoredResult | undefined;

    if (job.resultPath) {
      try {
        const raw = readFileSync(job.resultPath, 'utf-8');
        resultData = JSON.parse(raw) as StoredResult;
      } catch (err) {
        res.status(500).json({
          error: 'RESULT_READ_ERROR',
          message: `Failed to read results from disk: ${err instanceof Error ? err.message : String(err)}`,
        });
        return;
      }
    }

    // Client expects rows as arrays (indexed by column order), not objects
    const columns = resultData?.columns ?? [];
    const rows = (resultData?.rows ?? []).map(row =>
      columns.map(col => row[col.name])
    );

    res.status(200).json({
      queryExecutionId: job.jobId,
      status: job.status,
      results: {
        rows,
        columns: columns.map(col => col.name),
        format: 'array',
      },
      schema: { columns },
      rowCount: resultData?.rowCount ?? 0,
    });
  };
}
