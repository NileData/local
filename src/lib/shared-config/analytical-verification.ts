import type { AnalyticalPlan } from './analytical-planning.js';

export type AnalyticalStageResultStatus =
  | 'pass'
  | 'retry_same_stage'
  | 'backtrack'
  | 'clarification_needed'
  | 'abstain'
  | 'fail';

export interface AnalyticalVerificationStageRule {
  stage: string;
  checks: string[];
  onFailure: AnalyticalStageResultStatus;
}

export interface AnalyticalVerificationContract {
  requiredSections: string[];
  stageRules: AnalyticalVerificationStageRule[];
  finalChecks: string[];
}

export function getDefaultAnalyticalVerificationContract(
  analyticalPlan?: AnalyticalPlan | null
): AnalyticalVerificationContract {
  const planStages = analyticalPlan?.stages || [];

  return {
    requiredSections: ['verification status', 'failed checks', 'next action when verification fails'],
    stageRules: planStages.map((stage) => ({
      stage: stage.stage,
      checks: stage.verification,
      onFailure: stage.fallback === 'clarify'
        ? 'clarification_needed'
        : stage.fallback === 'backtrack'
          ? 'backtrack'
          : stage.fallback === 'retry_same_stage'
            ? 'retry_same_stage'
            : 'abstain',
    })),
    finalChecks: analyticalPlan?.verificationChecks?.length
      ? analyticalPlan.verificationChecks
      : [
          'confirm the answer numbers match the executed results',
          'confirm assumptions and caveats are surfaced',
          'confirm observed facts are separate from modeled scenarios',
        ],
  };
}

export function renderAnalyticalVerificationContract(
  contract: AnalyticalVerificationContract
): string {
  const requiredSections = contract.requiredSections.map((section) => `- ${section}`).join('\n');
  const stageRules = contract.stageRules.length
    ? contract.stageRules
      .map((rule) => {
        const checks = rule.checks.length
          ? rule.checks.map((check) => `  - ${check}`).join('\n')
          : '  - no explicit checks provided';
        return `- ${rule.stage}:\n${checks}\n  - on failure: ${rule.onFailure}`;
      })
      .join('\n')
    : '- no stage rules provided';
  const finalChecks = contract.finalChecks.map((check) => `- ${check}`).join('\n');

  return `Verification contract:
- required sections:
${requiredSections}
- stage rules:
${stageRules}
- final checks:
${finalChecks}`;
}
