import type { AnalyticalQuestionClass, AnalyticalRouteDecision } from './analytical-routing.js';

export type AnalyticalDeliverable = 'answer' | 'query' | 'plan';

export type AnalyticalStageName =
  | 'discover'
  | 'assess_sources'
  | 'plan_query'
  | 'execute_verify'
  | 'render';

export type AnalyticalStageExecutionMode = 'parallel_retrieval_only' | 'serialized';

export type AnalyticalStageFallback =
  | 'retry_same_stage'
  | 'backtrack'
  | 'clarify'
  | 'abstain';

export type AnalyticalWorker =
  | 'catalog_scout'
  | 'source_auditor'
  | 'sql_planner'
  | 'query_verifier';

export interface AnalyticalPlanStage {
  stage: AnalyticalStageName;
  goal: string;
  executionMode: AnalyticalStageExecutionMode;
  workers: AnalyticalWorker[];
  verification: string[];
  fallback: AnalyticalStageFallback;
}

export interface AnalyticalPlan {
  version: 1;
  objective: string;
  questionClass: AnalyticalQuestionClass;
  deliverable: AnalyticalDeliverable;
  mustClarifyBeforeExecution: boolean;
  clarificationTargets: string[];
  metrics: string[];
  dimensions: string[];
  assumptionsToValidate: string[];
  caveatsToSurface: string[];
  verificationChecks: string[];
  stages: AnalyticalPlanStage[];
  provisionalTitle?: string;
}

const TITLE_MAX_WORDS = 8;
const TITLE_MAX_CHARS = 60;

function normalizeStringList(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === 'string' && item.trim().length > 0)
    : [];
}

function normalizeStageName(value: unknown): AnalyticalStageName | null {
  switch (value) {
    case 'discover':
    case 'assess_sources':
    case 'plan_query':
    case 'execute_verify':
    case 'render':
      return value;
    default:
      return null;
  }
}

function getDefaultExecutionMode(stage: AnalyticalStageName): AnalyticalStageExecutionMode {
  return stage === 'discover' ? 'parallel_retrieval_only' : 'serialized';
}

function normalizeWorkerList(value: unknown): AnalyticalWorker[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value.filter((item): item is AnalyticalWorker => (
    item === 'catalog_scout'
    || item === 'source_auditor'
    || item === 'sql_planner'
    || item === 'query_verifier'
  ));
}

function normalizeFallback(value: unknown): AnalyticalStageFallback {
  switch (value) {
    case 'retry_same_stage':
    case 'backtrack':
    case 'clarify':
    case 'abstain':
      return value;
    default:
      return 'clarify';
  }
}

function normalizeDeliverable(value: unknown): AnalyticalDeliverable {
  switch (value) {
    case 'query':
    case 'plan':
      return value;
    default:
      return 'answer';
  }
}

function normalizeTitle(value: unknown): string | undefined {
  if (typeof value !== 'string') {
    return undefined;
  }

  const normalized = value
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/^['"`]+|['"`]+$/g, '')
    .replace(/[.!?]+$/g, '');

  if (!normalized) {
    return undefined;
  }

  const words = normalized.split(' ').filter(Boolean).slice(0, TITLE_MAX_WORDS);
  const joined = words.join(' ').slice(0, TITLE_MAX_CHARS).trim();
  return joined || undefined;
}

function defaultStages(): AnalyticalPlanStage[] {
  return [
    {
      stage: 'discover',
      goal: 'Find candidate sources relevant to the question.',
      executionMode: 'parallel_retrieval_only',
      workers: ['catalog_scout'],
      verification: ['Candidate sources cover the requested scope.'],
      fallback: 'clarify',
    },
    {
      stage: 'assess_sources',
      goal: 'Check grain, lineage, coverage, filters, and modeled-field risks.',
      executionMode: 'serialized',
      workers: ['source_auditor'],
      verification: ['Chosen sources are suitable for the claim being made.'],
      fallback: 'backtrack',
    },
    {
      stage: 'plan_query',
      goal: 'Define the smallest defensible query plan.',
      executionMode: 'serialized',
      workers: ['sql_planner'],
      verification: ['Plan preserves grain and requested comparison semantics.'],
      fallback: 'backtrack',
    },
    {
      stage: 'execute_verify',
      goal: 'Run bounded queries and validate the result before concluding.',
      executionMode: 'serialized',
      workers: ['query_verifier'],
      verification: ['Results are plausible and match the intended metric semantics.'],
      fallback: 'retry_same_stage',
    },
    {
      stage: 'render',
      goal: 'Answer concisely with evidence, assumptions, caveats, and verification status.',
      executionMode: 'serialized',
      workers: [],
      verification: ['Final answer separates observed facts from modeled scenarios.'],
      fallback: 'abstain',
    },
  ];
}

export function getDefaultAnalyticalPlan(routeDecision: AnalyticalRouteDecision): AnalyticalPlan {
  return {
    version: 1,
    objective: 'Answer the user question with explicit scope, evidence, assumptions, and caveats.',
    questionClass: routeDecision.questionClass,
    deliverable: 'answer',
    mustClarifyBeforeExecution: routeDecision.needsClarification,
    clarificationTargets: [...routeDecision.ambiguityReasons],
    metrics: [],
    dimensions: [],
    assumptionsToValidate: [],
    caveatsToSurface: [],
    verificationChecks: [],
    stages: defaultStages(),
    ...(normalizeTitle(routeDecision.provisionalTitle) ? { provisionalTitle: normalizeTitle(routeDecision.provisionalTitle) } : {}),
  };
}

export function normalizeAnalyticalPlan(raw: unknown, routeDecision: AnalyticalRouteDecision): AnalyticalPlan {
  if (!raw || typeof raw !== 'object') {
    return getDefaultAnalyticalPlan(routeDecision);
  }

  const record = raw as Record<string, unknown>;
  const rawStages = Array.isArray(record.stages) ? record.stages : [];
  const stages = rawStages
    .map((item) => {
      if (!item || typeof item !== 'object') {
        return null;
      }

      const stageRecord = item as Record<string, unknown>;
      const stage = normalizeStageName(stageRecord.stage);
      if (!stage || typeof stageRecord.goal !== 'string' || !stageRecord.goal.trim()) {
        return null;
      }

      return {
        stage,
        goal: stageRecord.goal.trim(),
        executionMode: stageRecord.executionMode === 'parallel_retrieval_only'
          ? 'parallel_retrieval_only'
          : getDefaultExecutionMode(stage),
        workers: normalizeWorkerList(stageRecord.workers),
        verification: normalizeStringList(stageRecord.verification),
        fallback: normalizeFallback(stageRecord.fallback),
      } satisfies AnalyticalPlanStage;
    })
    .filter((value): value is AnalyticalPlanStage => Boolean(value));

  const plan: AnalyticalPlan = {
    version: 1,
    objective: typeof record.objective === 'string' && record.objective.trim()
      ? record.objective.trim()
      : getDefaultAnalyticalPlan(routeDecision).objective,
    questionClass: typeof record.questionClass === 'string'
      ? record.questionClass as AnalyticalQuestionClass
      : routeDecision.questionClass,
    deliverable: normalizeDeliverable(record.deliverable),
    mustClarifyBeforeExecution: record.mustClarifyBeforeExecution === true,
    clarificationTargets: normalizeStringList(record.clarificationTargets),
    metrics: normalizeStringList(record.metrics),
    dimensions: normalizeStringList(record.dimensions),
    assumptionsToValidate: normalizeStringList(record.assumptionsToValidate),
    caveatsToSurface: normalizeStringList(record.caveatsToSurface),
    verificationChecks: normalizeStringList(record.verificationChecks),
    stages: stages.length > 0 ? stages : getDefaultAnalyticalPlan(routeDecision).stages,
    ...(normalizeTitle(record.provisionalTitle) ? { provisionalTitle: normalizeTitle(record.provisionalTitle) } : {}),
  };

  if (!plan.mustClarifyBeforeExecution && plan.clarificationTargets.length > 0) {
    plan.mustClarifyBeforeExecution = true;
  }

  return plan;
}

export function parseAnalyticalPlan(rawText: string, routeDecision: AnalyticalRouteDecision): AnalyticalPlan | null {
  const trimmed = rawText.trim();
  if (!trimmed) {
    return null;
  }

  try {
    return normalizeAnalyticalPlan(JSON.parse(trimmed), routeDecision);
  } catch {
    const jsonMatch = trimmed.match(/\{[\s\S]*\}/);
    if (!jsonMatch) {
      return null;
    }

    try {
      return normalizeAnalyticalPlan(JSON.parse(jsonMatch[0]), routeDecision);
    } catch {
      return null;
    }
  }
}

export const ANALYTICAL_PLANNER_PROMPT = `You create structured analytical execution plans.

Return exactly one JSON object and nothing else.

Your plan should:
- define the user's analytical objective in concrete terms
- decide whether clarification is needed before execution
- define what metrics and dimensions matter
- list assumptions that must be validated
- list caveats that must be surfaced if the answer is produced
- define explicit verification checks
- break the work into stages that can use the available subagents

Available workers:
- catalog_scout
- source_auditor
- sql_planner
- query_verifier

Output schema:
{
  "objective": "short objective",
  "questionClass": "business_impact_estimate" | "trend_analysis" | "segment_comparison" | "root_cause_analysis" | "metric_definition" | "source_selection" | "general_analytics",
  "deliverable": "answer" | "query" | "plan",
  "mustClarifyBeforeExecution": boolean,
  "clarificationTargets": ["specific ambiguity"],
  "metrics": ["metric name"],
  "dimensions": ["dimension name"],
  "assumptionsToValidate": ["assumption"],
  "caveatsToSurface": ["caveat"],
  "verificationChecks": ["verification check"],
  "stages": [
    {
      "stage": "discover" | "assess_sources" | "plan_query" | "execute_verify" | "render",
      "goal": "what this stage must accomplish",
      "executionMode": "parallel_retrieval_only" | "serialized",
      "workers": ["catalog_scout" | "source_auditor" | "sql_planner" | "query_verifier"],
      "verification": ["stage-local verification rule"],
      "fallback": "retry_same_stage" | "backtrack" | "clarify" | "abstain"
    }
  ],
  "provisionalTitle": "3-8 word topic title"
}

Rules:
- if ambiguity would materially change source choice, scope, comparator, or business semantics, set mustClarifyBeforeExecution=true
- stages must be concise and operational, not essay-like
- provisionalTitle and stage wording must be clear plain-language text, not slug IDs or internal shorthand
- prefer the smallest viable plan
- include at least one verification check per stage
- require Task-based delegation for every non-render analytical stage
- discover must use catalog_scout
- assess_sources must use source_auditor
- plan_query must use sql_planner
- execute_verify must use query_verifier
- render must not use a worker; it happens in the main thread only after the prior analysis stages complete
- only the discover stage may use "parallel_retrieval_only"; source auditing, query planning, verification, and rendering should remain serialized
- do not plan queries in parallel with source auditing; query planning starts only after acceptable sources are identified
- do not allow the parent analytical thread to perform discovery, source auditing, query planning, or query verification inline
- treat render as the only stage allowed to produce the final user-facing answer
- do not output markdown, comments, or explanation`;
