import type { ClarifyingQuestionRequest } from './clarifying-questions.js';
import type { AnalyticalPlan } from './analytical-planning.js';
import type { AnalyticalQuestionClass } from './analytical-routing.js';
import type { AnalyticalSourceRole } from './analytical-source-policy.js';
import type { AnalyticalStageResultStatus } from './analytical-verification.js';

export type AnalyticalVerificationStatus =
  | 'verified'
  | 'partially_verified'
  | 'unverified'
  | 'abstained';

export type AnalyticalRecommendedUse =
  | 'preferred'
  | 'allowed_with_caveats'
  | 'prioritization_only'
  | 'unsafe'
  | 'unknown';

export interface AnalyticalSourceAssessmentCandidate {
  name: string;
  reason: string;
  semanticRole?: AnalyticalSourceRole | 'unknown';
  grain?: string;
  recommendedUse?: AnalyticalRecommendedUse;
  riskFlags?: string[];
}

export interface AnalyticalSourceAssessmentRejectedCandidate {
  name: string;
  reason: string;
}

export interface AnalyticalSourceAssessment {
  status: AnalyticalStageResultStatus;
  selectedSources: AnalyticalSourceAssessmentCandidate[];
  rejectedSources: AnalyticalSourceAssessmentRejectedCandidate[];
  warnings: string[];
  assumptions: string[];
  caveats: string[];
  verificationNotes: string[];
  nextAction?: string;
  clarificationReason?: string;
}

export interface AnalyticalVerificationReport {
  status: AnalyticalStageResultStatus;
  verificationStatus: AnalyticalVerificationStatus;
  checksPassed: string[];
  checksFailed: string[];
  issues: string[];
  assumptions: string[];
  caveats: string[];
  nextAction?: string;
}

export interface AnalyticalRenderedAnswer {
  answer: string;
  supportingEvidence: string[];
  assumptions: string[];
  caveats: string[];
  verificationStatus: AnalyticalVerificationStatus;
  markdown: string;
}

function normalizeStringList(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === 'string' && item.trim().length > 0)
      .map((item) => item.trim())
    : [];
}

function normalizeStageStatus(value: unknown): AnalyticalStageResultStatus {
  switch (value) {
    case 'pass':
    case 'retry_same_stage':
    case 'backtrack':
    case 'clarification_needed':
    case 'abstain':
    case 'fail':
      return value;
    default:
      return 'pass';
  }
}

function normalizeVerificationStatus(value: unknown): AnalyticalVerificationStatus {
  switch (value) {
    case 'verified':
    case 'partially_verified':
    case 'unverified':
    case 'abstained':
      return value;
    default:
      return 'unverified';
  }
}

function normalizeRecommendedUse(value: unknown): AnalyticalRecommendedUse | undefined {
  switch (value) {
    case 'preferred':
    case 'allowed_with_caveats':
    case 'prioritization_only':
    case 'unsafe':
    case 'unknown':
      return value;
    default:
      return undefined;
  }
}

function normalizeSourceRole(value: unknown): AnalyticalSourceRole | 'unknown' | undefined {
  switch (value) {
    case 'official_source':
    case 'full_population_source':
    case 'aggregate':
    case 'report':
    case 'scenario':
    case 'modeled_output':
    case 'unknown':
      return value;
    default:
      return undefined;
  }
}

function normalizeSelectedSources(value: unknown): AnalyticalSourceAssessmentCandidate[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .map((candidate) => {
      if (!candidate || typeof candidate !== 'object') {
        return null;
      }

      const record = candidate as Record<string, unknown>;
      if (typeof record.name !== 'string' || !record.name.trim()) {
        return null;
      }

      return {
        name: record.name.trim(),
        reason: typeof record.reason === 'string' && record.reason.trim()
          ? record.reason.trim()
          : 'Selected as a plausible source.',
        ...(normalizeSourceRole(record.semanticRole) ? { semanticRole: normalizeSourceRole(record.semanticRole) } : {}),
        ...(typeof record.grain === 'string' && record.grain.trim() ? { grain: record.grain.trim() } : {}),
        ...(normalizeRecommendedUse(record.recommendedUse) ? { recommendedUse: normalizeRecommendedUse(record.recommendedUse) } : {}),
        ...(normalizeStringList(record.riskFlags).length > 0 ? { riskFlags: normalizeStringList(record.riskFlags) } : {}),
      } satisfies AnalyticalSourceAssessmentCandidate;
    })
    .filter((candidate): candidate is AnalyticalSourceAssessmentCandidate => Boolean(candidate));
}

function normalizeRejectedSources(value: unknown): AnalyticalSourceAssessmentRejectedCandidate[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .map((candidate) => {
      if (!candidate || typeof candidate !== 'object') {
        return null;
      }

      const record = candidate as Record<string, unknown>;
      if (typeof record.name !== 'string' || !record.name.trim()) {
        return null;
      }

      return {
        name: record.name.trim(),
        reason: typeof record.reason === 'string' && record.reason.trim()
          ? record.reason.trim()
          : 'Rejected during source assessment.',
      } satisfies AnalyticalSourceAssessmentRejectedCandidate;
    })
    .filter((candidate): candidate is AnalyticalSourceAssessmentRejectedCandidate => Boolean(candidate));
}

export function getDefaultAnalyticalSourceAssessment(): AnalyticalSourceAssessment {
  return {
    status: 'pass',
    selectedSources: [],
    rejectedSources: [],
    warnings: [],
    assumptions: [],
    caveats: [],
    verificationNotes: [],
  };
}

export function normalizeAnalyticalSourceAssessment(raw: unknown): AnalyticalSourceAssessment {
  if (!raw || typeof raw !== 'object') {
    return getDefaultAnalyticalSourceAssessment();
  }

  const record = raw as Record<string, unknown>;
  return {
    status: normalizeStageStatus(record.status),
    selectedSources: normalizeSelectedSources(record.selectedSources),
    rejectedSources: normalizeRejectedSources(record.rejectedSources),
    warnings: normalizeStringList(record.warnings),
    assumptions: normalizeStringList(record.assumptions),
    caveats: normalizeStringList(record.caveats),
    verificationNotes: normalizeStringList(record.verificationNotes),
    ...(typeof record.nextAction === 'string' && record.nextAction.trim()
      ? { nextAction: record.nextAction.trim() }
      : {}),
    ...(typeof record.clarificationReason === 'string' && record.clarificationReason.trim()
      ? { clarificationReason: record.clarificationReason.trim() }
      : {}),
  };
}

export function getDefaultAnalyticalVerificationReport(
  analyticalPlan?: AnalyticalPlan | null
): AnalyticalVerificationReport {
  return {
    status: 'pass',
    verificationStatus: 'unverified',
    checksPassed: [],
    checksFailed: [],
    issues: analyticalPlan?.verificationChecks?.length
      ? ['Verification stage did not return a structured report.']
      : [],
    assumptions: [],
    caveats: [],
  };
}

export function normalizeAnalyticalVerificationReport(
  raw: unknown,
  analyticalPlan?: AnalyticalPlan | null
): AnalyticalVerificationReport {
  if (!raw || typeof raw !== 'object') {
    return getDefaultAnalyticalVerificationReport(analyticalPlan);
  }

  const record = raw as Record<string, unknown>;
  return {
    status: normalizeStageStatus(record.status),
    verificationStatus: normalizeVerificationStatus(record.verificationStatus),
    checksPassed: normalizeStringList(record.checksPassed),
    checksFailed: normalizeStringList(record.checksFailed),
    issues: normalizeStringList(record.issues),
    assumptions: normalizeStringList(record.assumptions),
    caveats: normalizeStringList(record.caveats),
    ...(typeof record.nextAction === 'string' && record.nextAction.trim()
      ? { nextAction: record.nextAction.trim() }
      : {}),
  };
}

export function getDefaultAnalyticalRenderedAnswer(): AnalyticalRenderedAnswer {
  return {
    answer: '',
    supportingEvidence: [],
    assumptions: [],
    caveats: [],
    verificationStatus: 'unverified',
    markdown: '',
  };
}

export function normalizeAnalyticalRenderedAnswer(raw: unknown): AnalyticalRenderedAnswer {
  if (!raw || typeof raw !== 'object') {
    return getDefaultAnalyticalRenderedAnswer();
  }

  const record = raw as Record<string, unknown>;
  return {
    answer: typeof record.answer === 'string' ? record.answer.trim() : '',
    supportingEvidence: normalizeStringList(record.supportingEvidence),
    assumptions: normalizeStringList(record.assumptions),
    caveats: normalizeStringList(record.caveats),
    verificationStatus: normalizeVerificationStatus(record.verificationStatus),
    markdown: typeof record.markdown === 'string' ? record.markdown.trim() : '',
  };
}

export function parseAnalyticalJsonObject<T>(
  rawText: string,
  normalizer: (value: unknown) => T
): T | null {
  const trimmed = rawText.trim();
  if (!trimmed) {
    return null;
  }

  try {
    return normalizer(JSON.parse(trimmed));
  } catch {
    const jsonMatch = trimmed.match(/\{[\s\S]*\}/);
    if (!jsonMatch) {
      return null;
    }

    try {
      return normalizer(JSON.parse(jsonMatch[0]));
    } catch {
      return null;
    }
  }
}

export function parseAnalyticalClarificationRequest(rawText: string): ClarifyingQuestionRequest | null {
  const parsed = parseAnalyticalJsonObject(rawText, (value) => value);
  if (!parsed || typeof parsed !== 'object') {
    return null;
  }

  const record = parsed as Record<string, unknown>;
  if (!Array.isArray(record.questions)) {
    return null;
  }

  return record as unknown as ClarifyingQuestionRequest;
}

export const ANALYTICAL_CLARIFICATION_PROMPT = `You generate focused clarifying questions for analytical work.

Return exactly one JSON object and nothing else.

Goal:
- ask at most 2 short questions
- ask only when ambiguity would materially change source choice, scope, comparator, metric semantics, or business interpretation
- prefer concrete multiple-choice options over open-ended phrasing

Output schema:
{
  "questions": [
    {
      "question": "short question text",
      "header": "short label",
      "options": [
        { "label": "choice", "description": "what this means" },
        { "label": "choice", "description": "what this means" }
      ],
      "multiSelect": false
    }
  ],
  "metadata": {
    "source": "analytical_router" | "analytical_planner" | "source_assessment" | "verification"
  }
}

Rules:
- 1 question is preferred; 2 questions only when one question is not enough
- each question must have 2 to 4 options
- options should be specific and decision-useful, not generic
- if you can infer a reasonable recommended default, put it first
- do not ask for information already clearly provided in the conversation
- do not output markdown or explanation`;

export const ANALYTICAL_SOURCE_ASSESSMENT_PROMPT = `You assess analytical source suitability from compact source summaries.

Return exactly one JSON object and nothing else.

Output schema:
{
  "status": "pass" | "backtrack" | "clarification_needed" | "abstain" | "fail",
  "selectedSources": [
    {
      "name": "database.table",
      "reason": "why this source is acceptable",
      "semanticRole": "official_source" | "full_population_source" | "aggregate" | "report" | "scenario" | "modeled_output" | "unknown",
      "grain": "user/account/day/etc",
      "recommendedUse": "preferred" | "allowed_with_caveats" | "prioritization_only" | "unsafe" | "unknown",
      "riskFlags": ["risk"]
    }
  ],
  "rejectedSources": [
    {
      "name": "database.table",
      "reason": "why this source should not drive the answer"
    }
  ],
  "warnings": ["short warning"],
  "assumptions": ["assumption that still matters"],
  "caveats": ["caveat that must be surfaced"],
  "verificationNotes": ["stage-local verification note"],
  "nextAction": "what the workflow should do next",
  "clarificationReason": "only when clarification is required"
}

Rules:
- prefer sources explicitly supported by the supplied summaries
- use the source-selection policy and question class to decide whether a source is acceptable
- do not silently treat scenario, report, or modeled-output tables as canonical empirical evidence
- if no source is clearly good enough, return status=clarification_needed, backtrack, or abstain rather than forcing a confident selection
- this stage does not render the user-facing answer; it only decides source suitability and the next workflow action
- if source assessment is incomplete or unsafe, direct the workflow to clarify, backtrack, or abstain rather than allowing final rendering
- keep reasons and notes short and specific
- do not output markdown or explanation`;

export const ANALYTICAL_VERIFIER_PROMPT = `You verify a draft analytical answer before it is shown to the user.

Return exactly one JSON object and nothing else.

Output schema:
{
  "status": "pass" | "retry_same_stage" | "backtrack" | "clarification_needed" | "abstain" | "fail",
  "verificationStatus": "verified" | "partially_verified" | "unverified" | "abstained",
  "checksPassed": ["short check"],
  "checksFailed": ["short check"],
  "issues": ["specific issue"],
  "assumptions": ["assumption that remains important"],
  "caveats": ["caveat that must appear in the final answer"],
  "nextAction": "what the workflow should do next"
}

Rules:
- verify against the supplied analytical plan, source assessment, and compact execution evidence
- if the draft answer overclaims beyond the evidence, mark that explicitly
- if the draft depends on unresolved ambiguity or weak sources, prefer partially_verified, unverified, or abstained
- do not invent checks that are not supported by the supplied context
- this stage must not rewrite or render the final user-facing answer; it only approves, rejects, or caveats whether rendering may proceed
- if verification is not strong enough for final rendering, set the next action to backtrack, clarify, retry, or abstain rather than allowing a confident answer through
- keep outputs compact and operational
- do not output markdown or explanation`;

export const ANALYTICAL_RENDERER_PROMPT = `You render the final user-facing analytical answer from structured review artifacts.

Return exactly one JSON object and nothing else.

Output schema:
{
  "answer": "short direct answer paragraph",
  "supportingEvidence": ["bullet-sized evidence item"],
  "assumptions": ["short assumption"],
  "caveats": ["short caveat"],
  "verificationStatus": "verified" | "partially_verified" | "unverified" | "abstained",
  "markdown": "final markdown answer"
}

Rules:
- lead with the answer, not setup
- keep the final answer concise and high signal
- preserve assumptions, caveats, and verification status when they materially affect the conclusion
- separate observed facts from modeled or hypothetical claims
- treat this as the only stage that may produce the final user-facing answer
- assume all analysis, source assessment, and verification work is already complete before rendering
- if evidence is insufficient, markdown should abstain or clearly caveat the result instead of sounding confident
- supportingEvidence, assumptions, and caveats should be non-redundant
- markdown should be user-facing and easy to scan
- do not output markdown outside the JSON object`;

export function renderAnalyticalExecutionContextHeader(
  routeDecision: { questionClass: AnalyticalQuestionClass },
  analyticalPlan?: AnalyticalPlan | null
): string {
  return [
    `Question class: ${routeDecision.questionClass}`,
    analyticalPlan?.objective ? `Objective: ${analyticalPlan.objective}` : null,
    analyticalPlan?.verificationChecks?.length
      ? `Verification checks:\n${analyticalPlan.verificationChecks.map((check) => `- ${check}`).join('\n')}`
      : null,
  ].filter(Boolean).join('\n');
}
