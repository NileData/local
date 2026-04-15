import type { RequestHandler } from 'express';
import type { QueryWorkerPool } from '../../services/query-worker-pool.js';
import type { SQLiteService } from '../../services/sqlite-service.js';

export function createQueryStopHandler(
  queryWorkerPool: QueryWorkerPool,
  sqliteService: SQLiteService,
): RequestHandler {
  return async (req, res) => {
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

    if (job.status === "COMPLETED" || job.status === "FAILED" || job.status === "CANCELLED") {
      res.status(200).json({
        queryExecutionId: job.jobId,
        status: job.status,
        message: `Query already in terminal state: ${job.status}`,
      });
      return;
    }

    await queryWorkerPool.cancel(queryExecutionId);

    res.status(200).json({
      queryExecutionId: job.jobId,
      status: "CANCELLED",
      message: "Query execution cancelled.",
    });
  };
}
