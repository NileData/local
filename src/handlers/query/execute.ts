import { randomUUID } from 'crypto';
import type { RequestHandler } from 'express';
import type { ExecuteQueryRequest, ExecuteQueryResponse } from '../../types/types.js';
import type { LocalConfig } from '../../config/environment.js';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { QueryWorkerPool } from '../../services/query-worker-pool.js';
import { substituteParameters } from '../../services/parameter-substitution-service.js';
import { resolveSystemVariables } from '../../services/system-parameter-service.js';

export function createQueryExecuteHandler(
  config: LocalConfig,
  queryWorkerPool: QueryWorkerPool,
  sqliteService: SQLiteService,
): RequestHandler {
  return (req, res) => {
    const body = req.body as ExecuteQueryRequest | undefined;

    if (!body || !body.query || body.query.trim() === '') {
      res.status(400).json({
        error: 'VALIDATION_ERROR',
        message: 'Request body must include a non-empty "query" field.',
      });
      return;
    }

    const originalQuery = body.query;
    const language = body.language ?? 'sql';
    const parameters = body.parameters;

    if (!config.compute.sparkEnabled) {
      res.status(503).json({
        error: 'ENGINE_UNAVAILABLE',
        message: 'Spark is not enabled in local mode.',
      });
      return;
    }

    // Perform parameter substitution before execution
    let sql = originalQuery;

    // Step 1: Resolve system variables (@ScheduleDate, @ScheduleTime)
    sql = resolveSystemVariables(sql, { scheduledTime: new Date() });

    // Step 2: Substitute user-defined parameters
    if (parameters && parameters.length > 0) {
      try {
        sql = substituteParameters(sql, parameters);
      } catch (err) {
        res.status(400).json({
          error: 'PARAMETER_ERROR',
          message: err instanceof Error ? err.message : String(err),
        });
        return;
      }
    }

    const queryExecutionId = `qe-${Date.now()}-${randomUUID().slice(0, 8)}`;
    const now = new Date().toISOString();

    // Insert job record
    sqliteService.insertJob({
      jobId: queryExecutionId,
      type: "query",
      status: "QUEUED",
      engine: 'spark',
      inputJson: JSON.stringify({ query: sql, originalQuery, language, engine: 'spark', parameters }),
      submittedAt: now,
    });

    queryWorkerPool.submit({
      jobId: queryExecutionId,
      sql,
      language: language === 'python' ? 'python' : 'sql',
      resultSetLimit: body.resultSetLimit,
    });

    const runtimeState = queryWorkerPool.getRuntimeState(queryExecutionId);
    const response: ExecuteQueryResponse & {
      queuePosition?: number;
      queuedCountAhead?: number;
    } = {
      queryExecutionId,
      status: "QUEUED",
      message: 'Query submitted to local Spark worker pool.',
      ...(runtimeState?.queuePosition !== undefined ? { queuePosition: runtimeState.queuePosition } : {}),
      ...(runtimeState?.queuedCountAhead !== undefined ? { queuedCountAhead: runtimeState.queuedCountAhead } : {}),
    };
    res.status(200).json(response);
  };
}
