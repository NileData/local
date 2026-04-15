import type { AnalyticalPlan } from './analytical-planning.js';
import {
  getDefaultAnalyticalAnswerContract,
  renderAnalyticalAnswerContract,
} from './analytical-answer.js';
import { renderAnalyticalStageExecutionPolicy } from './analytical-stage-policy.js';
import {
  getSourceSelectionPolicy,
  renderSourceSelectionPolicy,
} from './analytical-source-policy.js';
import {
  getDefaultAnalyticalVerificationContract,
  renderAnalyticalVerificationContract,
} from './analytical-verification.js';

export type ChatExecutionMode = 'general_chat' | 'analytical';

export type AnalyticalQuestionClass =
  | 'general_chat'
  | 'business_impact_estimate'
  | 'trend_analysis'
  | 'segment_comparison'
  | 'root_cause_analysis'
  | 'metric_definition'
  | 'source_selection'
  | 'general_analytics';

export interface AnalyticalRouteDecision {
  route: ChatExecutionMode;
  questionClass: AnalyticalQuestionClass;
  confidence: number;
  needsClarification: boolean;
  ambiguityReasons: string[];
  provisionalTitle?: string;
}

const TITLE_MAX_WORDS = 8;
const TITLE_MAX_CHARS = 60;

function clampConfidence(value: unknown): number {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    return 0.5;
  }
  return Math.min(1, Math.max(0, value));
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

export function getDefaultAnalyticalRouteDecision(): AnalyticalRouteDecision {
  return {
    route: 'general_chat',
    questionClass: 'general_chat',
    confidence: 0.5,
    needsClarification: false,
    ambiguityReasons: [],
  };
}

export function normalizeAnalyticalRouteDecision(raw: unknown): AnalyticalRouteDecision {
  if (!raw || typeof raw !== 'object') {
    return getDefaultAnalyticalRouteDecision();
  }

  const record = raw as Record<string, unknown>;
  const route = record.route === 'analytical' ? 'analytical' : 'general_chat';
  const questionClass = typeof record.questionClass === 'string'
    ? record.questionClass as AnalyticalQuestionClass
    : route === 'analytical'
      ? 'general_analytics'
      : 'general_chat';
  const ambiguityReasons = Array.isArray(record.ambiguityReasons)
    ? record.ambiguityReasons.filter((value): value is string => typeof value === 'string' && value.trim().length > 0)
    : [];

  return {
    route,
    questionClass,
    confidence: clampConfidence(record.confidence),
    needsClarification: record.needsClarification === true,
    ambiguityReasons,
    ...(normalizeTitle(record.provisionalTitle) ? { provisionalTitle: normalizeTitle(record.provisionalTitle) } : {}),
  };
}

export function parseAnalyticalRouteDecision(rawText: string): AnalyticalRouteDecision | null {
  const trimmed = rawText.trim();
  if (!trimmed) {
    return null;
  }

  try {
    return normalizeAnalyticalRouteDecision(JSON.parse(trimmed));
  } catch {
    const jsonMatch = trimmed.match(/\{[\s\S]*\}/);
    if (!jsonMatch) {
      return null;
    }

    try {
      return normalizeAnalyticalRouteDecision(JSON.parse(jsonMatch[0]));
    } catch {
      return null;
    }
  }
}

export const ANALYTICAL_ROUTE_PROMPT = `You classify the execution mode for a chat request.

Return exactly one JSON object and nothing else.

Your job:
- decide whether the request should stay in "general_chat" mode or use "analytical" mode
- assign the best questionClass
- estimate confidence
- decide whether a clarifying question is needed before deeper work
- provide a short provisionalTitle when helpful

Use "analytical" when the request requires business analysis, trends, comparisons, segments, cohorts, impact estimates, root-cause analysis, source-selection judgment, or defensible query planning.

Use "general_chat" when the request is primarily:
- schema lookup
- simple table or row inspection
- lineage exploration
- job/status lookup
- straightforward transform explanation

Output schema:
{
  "route": "general_chat" | "analytical",
  "questionClass": "general_chat" | "business_impact_estimate" | "trend_analysis" | "segment_comparison" | "root_cause_analysis" | "metric_definition" | "source_selection" | "general_analytics",
  "confidence": 0.0-1.0,
  "needsClarification": boolean,
  "ambiguityReasons": ["short reason"],
  "provisionalTitle": "3-8 word title"
}

Rules:
- prefer "analytical" when the answer depends on interpretation, comparison, or source choice
- set needsClarification=true only when the ambiguity would materially change the answer
- ambiguityReasons must be short and specific
- provisionalTitle should describe the user topic, not assistant actions
- no markdown, no explanation, no surrounding text`;

export function applyExecutionModePrompt(
  basePrompt: string,
  executionMode?: ChatExecutionMode,
  routeDecision?: AnalyticalRouteDecision | null,
  analyticalPlan?: AnalyticalPlan | null
): string {
  if (executionMode !== 'analytical') {
    return basePrompt;
  }

  const questionClass = routeDecision?.questionClass || 'general_analytics';
  const sourcePolicy = getSourceSelectionPolicy(questionClass);
  const verificationContract = getDefaultAnalyticalVerificationContract(analyticalPlan);
  const answerContract = getDefaultAnalyticalAnswerContract();
  const ambiguityReasons = routeDecision?.ambiguityReasons?.length
    ? routeDecision.ambiguityReasons.map((reason) => `- ${reason}`).join('\n')
    : '- none recorded';
  const planObjective = analyticalPlan?.objective || 'Not yet planned';
  const planMetrics = analyticalPlan?.metrics?.length
    ? analyticalPlan.metrics.map((metric) => `- ${metric}`).join('\n')
    : '- none specified';
  const planDimensions = analyticalPlan?.dimensions?.length
    ? analyticalPlan.dimensions.map((dimension) => `- ${dimension}`).join('\n')
    : '- none specified';
  const planVerificationChecks = analyticalPlan?.verificationChecks?.length
    ? analyticalPlan.verificationChecks.map((check) => `- ${check}`).join('\n')
    : '- verify source suitability, result plausibility, and caveat requirements';
  const stageSummary = analyticalPlan?.stages?.length
    ? analyticalPlan.stages
      .map((stage) => `- ${stage.stage} [${stage.executionMode}]: ${stage.goal}`)
      .join('\n')
    : '- discover [parallel_retrieval_only]\n- assess_sources [serialized]\n- plan_query [serialized]\n- execute_verify [serialized]\n- render [serialized]';

  return `${basePrompt}

# Analytical Mode

This conversation is routed into analytical mode. Follow a staged analytical workflow rather than a free-form answer loop.

Execution contract:
1. Discover candidate sources
2. Assess source quality, grain, scope, and modeled-field risks
3. Choose sources and define scope explicitly
4. Plan the minimum viable query or query set
5. Execute only what is needed
6. Verify before concluding
7. Render a concise answer with evidence, assumptions, and caveats

Current route context:
- execution mode: analytical
- question class: ${questionClass}
- needs clarification: ${routeDecision?.needsClarification === true ? 'yes' : 'no'}
- ambiguity reasons:
${ambiguityReasons}

Analytical plan:
- objective: ${planObjective}
- must clarify before execution: ${analyticalPlan?.mustClarifyBeforeExecution === true ? 'yes' : 'no'}
- deliverable: ${analyticalPlan?.deliverable || 'answer'}
- planned stages:
${stageSummary}
- metrics:
${planMetrics}
- dimensions:
${planDimensions}
- verification checks:
${planVerificationChecks}

${renderSourceSelectionPolicy(sourcePolicy)}

${renderAnalyticalStageExecutionPolicy(analyticalPlan)}

${renderAnalyticalVerificationContract(verificationContract)}

${renderAnalyticalAnswerContract(answerContract)}

Working rules:
- the main analytical thread is an orchestrator: it may clarify, coordinate stages, and render the final answer, but it must not perform discovery, source auditing, query planning, or query verification inline
- for every non-render stage, launch a Task/Agent subtask instead of calling the stage tools directly in the main thread
- discover must run inside a catalog_scout Task
- assess_sources must run inside a source_auditor Task
- plan_query must run inside a sql_planner Task
- execute_verify must run inside a query_verifier Task
- when launching a subtask/subagent, give it a clear user-friendly plain-language title because the user will see it in the UI
- parallelize retrieval only inside discovery; keep source assessment, source selection, query planning, verification, and rendering serialized
- never audit sources and plan the query in parallel; query planning starts only after acceptable sources are identified
- do not guess through material ambiguity; ask one focused clarifying question with the user-question tool when needed
- do not draft the final user-facing answer during discovery, source assessment, query planning, or verification; keep those stages inside task outputs only
- render exactly one final user-facing answer only after source assessment and verification are complete or an abstention is explicitly chosen
- separate observed facts from modeled scenarios
- be concise in the final answer, but do not omit assumptions, caveats, or verification findings
- if verification fails, backtrack or clarify instead of pushing through to a confident answer`;
}
