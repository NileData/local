import type { RequestHandler, Request, Response } from 'express';
import type { SparkEngine } from '../../engines/spark-engine.js';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { LocalConfig } from '../../config/environment.js';
import type { SaveQueryAsTableRequest, QueryParameter } from '../../types/types.js';
import { createTableCreateHandler } from '../table/create.js';
import { substituteParameters } from '../../services/parameter-substitution-service.js';
import { resolveSystemVariables } from '../../services/system-parameter-service.js';
import { validateSATRequest, httpStatusForError, buildDepExtractionCode, parseDepExtractionResult } from '../../lib/shared-api/index.js';

/**
 * Save As Table handler:
 * 1. Parse SaveQueryAsTableRequest
 * 2. Extract SQL from code.content
 * 3. Build a CreateTable-compatible request
 * 4. Delegate to the CreateTable handler internally
 *
 * The CreateTable handler handles namespace creation, CTAS, schema extraction,
 * and catalog insertion. This handler is purely orchestration.
 */
export function createSaveAsTableHandler(config: LocalConfig, sparkEngine: SparkEngine, sqliteService: SQLiteService): RequestHandler {
  const createTableHandler = createTableCreateHandler(config, sparkEngine, sqliteService);

  return async (req, res) => {
    const body = req.body as Partial<SaveQueryAsTableRequest> | undefined;
    if (!body) {
      res.status(400).json({ error: 'VALIDATION_ERROR', message: 'Request body is required.' });
      return;
    }

    // Validate required fields
    const validationError = validateSATRequest(body);
    if (validationError) {
      res.status(httpStatusForError(validationError)).json({
        error: validationError.errorCode,
        message: validationError.message,
      });
      return;
    }

    // Reject scheduled jobs -- local mode doesn't support scheduling
    const hasSchedule = body.jobs?.some(j =>
      j.schedule && j.schedule.type !== 'none' && j.schedule.type !== 'never' && j.schedule.cron
    );
    if (hasSchedule) {
      res.status(400).json({
        error: 'NOT_AVAILABLE_IN_LOCAL_MODE',
        message: 'Scheduled jobs are not available in local mode. Remove the schedule or use Nile cloud for scheduled ETL.',
      });
      return;
    }

    // After validation, these fields are guaranteed present
    const code = body.code!;
    const queryExecutionId = body.queryExecutionId!;
    const tableName = body.tableName!;

    // Extract SQL from code.content or look up from original query
    let sql: string | undefined = code.content;
    if (!sql) {
      const originalJob = sqliteService.getJob(queryExecutionId);
      if (originalJob?.inputJson) {
        try {
          const input = JSON.parse(originalJob.inputJson) as { query?: string };
          sql = input.query;
        } catch {
          // Non-critical
        }
      }
    }

    if (!sql) {
      res.status(400).json({
        error: 'MISSING_CODE_CONTENT',
        message: 'Could not determine query code from code.content or query history.',
      });
      return;
    }

    // Resolve parameters before execution
    // Step 1: Resolve system variables (@ScheduleDate, @ScheduleTime)
    sql = resolveSystemVariables(sql, { scheduledTime: new Date() });

    // Step 2: Substitute user-defined parameters
    const codeParams = code.parameters;
    if (codeParams && Array.isArray(codeParams) && codeParams.length > 0) {
      try {
        const queryParams: QueryParameter[] = codeParams.map(p => ({
          name: p.name,
          paramType: p.paramType || 'string',
          value: p.defaultValue || '',
          isUsed: true,
        }));
        sql = substituteParameters(sql, queryParams);
      } catch (err) {
        res.status(400).json({
          error: 'PARAMETER_ERROR',
          message: err instanceof Error ? err.message : String(err),
        });
        return;
      }
    }

    // Build CreateTable-compatible request body and delegate
    const database = (body as Record<string, unknown>).database as string || 'local';

    // Extract actual dependencies using Spark's analyzed query plan.
    // This runs the SQL through Spark to get the plan, then parses table references.
    // Non-blocking: if extraction fails, we proceed with empty deps.
    let actualDeps: Awaited<ReturnType<typeof parseDepExtractionResult>> = [];
    try {
      const depCode = buildDepExtractionCode(sql);
      const depResult = await sparkEngine.executePython(depCode);
      actualDeps = parseDepExtractionResult(depResult.rows);
    } catch (err) {
      console.warn('[save-as-table] Dependency extraction failed (non-blocking):', err instanceof Error ? err.message : err);
    }

    const createTableBody = {
      database,
      tableName,
      description: body.description || `Created from query ${queryExecutionId}`,
      sourceSql: sql,
      updateStrategy: body.updateStrategy || 'replace',
      // Pass code object through so CreateTable stores it in the definition
      code: {
        content: sql,
        language: code.language || 'sql',
        dialect: code.dialect || 'spark-sql',
        parameters: code.parameters,
        systemParameters: code.systemParameters,
      },
      // Pass extracted dependencies so CreateTable stores them in definition
      dependencies: {
        tables: {
          declared: body.dependencies?.tables?.map(t => ({ database: t.database, table: t.table })) || [],
          actual: actualDeps.map(d => ({
            database: d.database,
            table: d.table,
            confidence: d.confidence,
            detectionMethod: d.detectionMethod,
          })),
        },
        actualDepsVersion: actualDeps.length > 0 ? '1.0' : undefined,
        actualDepsCapturedAt: actualDeps.length > 0 ? new Date().toISOString() : undefined,
      },
    };

    // Create a synthetic request to forward to CreateTable handler
    const syntheticReq = {
      ...req,
      body: createTableBody,
    } as Request;

    // Delegate to CreateTable -- it handles everything
    await createTableHandler(syntheticReq, res, () => { /* noop next */ });
  };
}
