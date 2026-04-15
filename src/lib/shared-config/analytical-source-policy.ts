import type { AnalyticalQuestionClass } from './analytical-routing.js';

export type AnalyticalSourceRole =
  | 'official_source'
  | 'full_population_source'
  | 'aggregate'
  | 'report'
  | 'scenario'
  | 'modeled_output';

export interface AnalyticalSourcePolicy {
  questionClass: AnalyticalQuestionClass;
  preferredRoles: AnalyticalSourceRole[];
  allowedRoles: AnalyticalSourceRole[];
  discouragedSignals: string[];
  requiredChecks: string[];
  escalationGuidance: string[];
}

function buildBasePolicy(questionClass: AnalyticalQuestionClass): AnalyticalSourcePolicy {
  return {
    questionClass,
    preferredRoles: ['official_source', 'full_population_source'],
    allowedRoles: ['official_source', 'full_population_source', 'aggregate'],
    discouragedSignals: [
      'rank-filtered or top-N tables used for whole-population claims',
      'modeled output columns presented as observed facts',
      'scenario or prioritization tables used as canonical empirical evidence',
    ],
    requiredChecks: [
      'confirm the table grain matches the claim',
      'confirm scope and population coverage are sufficient',
      'confirm filters do not silently narrow the result set',
    ],
    escalationGuidance: [
      'if strong candidates disagree on scope or semantics, audit them before choosing',
      'if no source is clearly canonical, ask a clarifying question or caveat the answer',
    ],
  };
}

export function getSourceSelectionPolicy(questionClass: AnalyticalQuestionClass): AnalyticalSourcePolicy {
  const policy = buildBasePolicy(questionClass);

  switch (questionClass) {
    case 'business_impact_estimate':
      return {
        ...policy,
        preferredRoles: ['official_source', 'full_population_source'],
        allowedRoles: ['official_source', 'full_population_source', 'aggregate'],
        discouragedSignals: [
          ...policy.discouragedSignals,
          'segment-only tables used to estimate whole-business impact',
          'scenario tables treated as measured outcome tables',
        ],
        requiredChecks: [
          ...policy.requiredChecks,
          'confirm comparator semantics and business scope before estimating impact',
          'separate observed facts from modeled scenarios',
        ],
      };
    case 'trend_analysis':
      return {
        ...policy,
        preferredRoles: ['official_source', 'full_population_source', 'aggregate'],
        allowedRoles: ['official_source', 'full_population_source', 'aggregate', 'report'],
        requiredChecks: [
          ...policy.requiredChecks,
          'confirm time grain and time window are explicit',
        ],
      };
    case 'segment_comparison':
      return {
        ...policy,
        preferredRoles: ['official_source', 'full_population_source', 'aggregate'],
        allowedRoles: ['official_source', 'full_population_source', 'aggregate', 'report'],
        requiredChecks: [
          ...policy.requiredChecks,
          'confirm segmentation logic and comparison groups are explicit',
        ],
      };
    case 'root_cause_analysis':
      return {
        ...policy,
        preferredRoles: ['official_source', 'full_population_source'],
        allowedRoles: ['official_source', 'full_population_source', 'aggregate', 'report'],
        requiredChecks: [
          ...policy.requiredChecks,
          'distinguish correlation from causal inference',
          'look for missing slices, join fanout, or silent filter bias before concluding',
        ],
      };
    case 'metric_definition':
      return {
        ...policy,
        preferredRoles: ['official_source', 'aggregate'],
        allowedRoles: ['official_source', 'aggregate', 'report'],
        discouragedSignals: [
          ...policy.discouragedSignals,
          'derived or presentation-only metrics treated as official definitions without corroboration',
        ],
      };
    case 'source_selection':
      return {
        ...policy,
        preferredRoles: ['official_source', 'full_population_source'],
        allowedRoles: ['official_source', 'full_population_source', 'aggregate', 'report'],
        requiredChecks: [
          ...policy.requiredChecks,
          'compare candidate lineage and intended use explicitly',
        ],
      };
    case 'general_analytics':
      return {
        ...policy,
        allowedRoles: ['official_source', 'full_population_source', 'aggregate', 'report'],
      };
    case 'general_chat':
    default:
      return policy;
  }
}

export function renderSourceSelectionPolicy(policy: AnalyticalSourcePolicy): string {
  const preferredRoles = policy.preferredRoles.map((role) => `- ${role}`).join('\n');
  const allowedRoles = policy.allowedRoles.map((role) => `- ${role}`).join('\n');
  const discouragedSignals = policy.discouragedSignals.map((signal) => `- ${signal}`).join('\n');
  const requiredChecks = policy.requiredChecks.map((check) => `- ${check}`).join('\n');
  const escalationGuidance = policy.escalationGuidance.map((line) => `- ${line}`).join('\n');

  return `Source-selection policy for ${policy.questionClass}:
- preferred roles:
${preferredRoles}
- allowed roles:
${allowedRoles}
- discouraged signals:
${discouragedSignals}
- required checks:
${requiredChecks}
- escalation guidance:
${escalationGuidance}`;
}
