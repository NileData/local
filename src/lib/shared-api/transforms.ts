/**
 * Shared Request Transformations
 *
 * Pure functions for transforming SAT requests into CreateTable requests.
 * Used by both cloud and local API handlers.
 *
 * Extracted from the save-as-table handler.
 */

/** Minimal job shape for filtering (avoids importing full generated types) */
interface JobInput {
  name: string;
  enabled?: boolean;
  schedule: { type: string; cron?: string };
  targetPartition?: { type: string; partitionValues?: unknown };
  dependencyMappings?: {
    tablePartitions?: Array<{
      database?: string;
      table: string;
      partitionMapping: { type: string; column?: string; partitionValues?: unknown };
    }>;
    eventFilters?: Array<{ eventArn: string; conditions?: string }>;
  };
  parameterValues?: unknown[];
}

/** Output job definition shape */
export interface JobDefinitionOutput {
  jobId: string;
  name: string;
  enabled: boolean;
  schedule: { type: string; cron?: string };
  targetPartition: { type: string; partitionValues?: unknown };
  dependencyMappings?: {
    tablePartitions?: Array<{
      database: string;
      table: string;
      partitionMapping: { type: string; column?: string };
    }>;
    eventFilters?: Array<{ eventArn: string; conditions?: string }>;
  };
  parameterValues: unknown[];
}

/**
 * Filter jobs to only those with valid schedules.
 * Jobs with schedule.type === 'never' or empty cron are excluded.
 *
 * From cloud SAT handler lines 155-157.
 */
export function filterJobsWithSchedules(jobs: JobInput[]): JobInput[] {
  return jobs.filter(
    (job) => job.schedule?.type !== 'never' && job.schedule?.cron && job.schedule.cron !== ''
  );
}

/**
 * Map filtered jobs to JobDefinition format with temporary IDs.
 *
 * From cloud SAT handler lines 160-186.
 */
export function mapJobsToDefinitions(jobs: JobInput[]): JobDefinitionOutput[] {
  return jobs.map((job, index) => ({
    jobId: `job-${index + 1}`,
    name: job.name,
    enabled: job.enabled !== false,
    schedule: {
      type: job.schedule.type,
      cron: job.schedule.cron,
    },
    targetPartition: job.targetPartition || { type: 'FULL_TABLE' },
    dependencyMappings: job.dependencyMappings
      ? {
          tablePartitions: job.dependencyMappings.tablePartitions?.map((tp) => ({
            database: tp.database || '',
            table: tp.table,
            partitionMapping: {
              type: tp.partitionMapping.type,
              column: tp.partitionMapping.column,
            },
          })),
          eventFilters: job.dependencyMappings.eventFilters?.map((ef) => ({
            eventArn: ef.eventArn,
            conditions: ef.conditions,
          })),
        }
      : undefined,
    parameterValues: job.parameterValues || [],
  }));
}

/** Dependencies input shape */
interface DependenciesInput {
  tables?: Array<{ database: string; table: string }>;
  externalEvents?: Array<{ arn: string; conditions?: string }>;
}

/** Actual dependency shape */
interface ActualDep {
  database: string;
  table: string;
  confidence: number;
  detectionMethod: string;
}

/** Parameters for building a CreateTable request from SAT */
export interface BuildCreateTableParams {
  tableName: string;
  description?: string;
  queryExecutionId: string;
  code: {
    content: string;
    language: string;
    dialect?: string;
    parameters?: unknown[];
    systemParameters?: unknown[];
  };
  schema?: unknown[];
  partitionColumns?: string[];
  primaryKey?: string[];
  updateStrategy?: string;
  dependencies?: DependenciesInput;
  actualDeps?: ActualDep[];
  jobs: JobDefinitionOutput[];
  initialLoad?: boolean;
}

/**
 * Build a CreateTableRequest from SAT parameters.
 *
 * From cloud SAT handler lines 189-234.
 * Returns a plain object suitable for the CreateTable handler.
 */
export function buildCreateTableFromSAT(params: BuildCreateTableParams): Record<string, unknown> {
  const request: Record<string, unknown> = {
    table: params.tableName,
    tableType: 'managed',
    definition: {
      schema: params.schema || [],
      description: params.description || `Created from query ${params.queryExecutionId}`,
      partitionColumns: params.partitionColumns || [],
      primaryKey: params.primaryKey || [],
      updateStrategy: params.updateStrategy || 'replace',
      code: {
        content: params.code.content,
        language: params.code.language,
        dialect: params.code.dialect,
        parameters: params.code.parameters,
        systemParameters: params.code.systemParameters,
      },
      dependencies: {
        tables: {
          declared: params.dependencies?.tables?.map((t) => ({
            database: t.database,
            table: t.table,
          })) || [],
          actual: (params.actualDeps || []).map((d) => ({
            database: d.database,
            table: d.table,
            confidence: d.confidence,
            detectionMethod: d.detectionMethod,
          })),
        },
        externalEvents: params.dependencies?.externalEvents?.map((e) => ({
          arn: e.arn,
          conditions: e.conditions ? JSON.parse(e.conditions) : undefined,
        })) || [],
        actualDepsVersion: params.actualDeps && params.actualDeps.length > 0 ? '1.0' : undefined,
        actualDepsCapturedAt: params.actualDeps && params.actualDeps.length > 0 ? new Date().toISOString() : undefined,
      },
      jobs: params.jobs,
    },
  };

  if (params.initialLoad) {
    request.initialData = { shouldLoad: true };
  }

  return request;
}
