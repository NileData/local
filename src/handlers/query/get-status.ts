import type { RequestHandler } from 'express';
import type { QueryWorkerPool } from '../../services/query-worker-pool.js';
import type { SQLiteService } from '../../services/sqlite-service.js';

export function createQueryStatusHandler(
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

    res.status(200).json({
      queryExecutionId: job.jobId,
      status: job.status,
      message: job.errorMessage ?? undefined,
      engine: job.engine,
      submittedAt: job.submittedAt,
      completedAt: job.completedAt ?? undefined,
      ...(runtimeState?.queuePosition !== undefined ? { queuePosition: runtimeState.queuePosition } : {}),
      ...(runtimeState?.queuedCountAhead !== undefined ? { queuedCountAhead: runtimeState.queuedCountAhead } : {}),
      ...(runtimeState?.workerId ? { workerId: runtimeState.workerId } : {}),
    });
  };
}
