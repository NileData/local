import type { AnalyticalPlan } from './analytical-planning.js';

export interface AnalyticalStageExecutionRule {
  stage: string;
  executionMode: 'parallel_retrieval_only' | 'serialized';
  prerequisite: string;
  guidance: string;
}

export function getDefaultAnalyticalStageExecutionRules(
  analyticalPlan?: AnalyticalPlan | null
): AnalyticalStageExecutionRule[] {
  const stages = analyticalPlan?.stages || [];

  return stages.map((stage, index) => {
    const previousStage = stages[index - 1];
    const prerequisite = previousStage
      ? `Finish ${previousStage.stage} before starting ${stage.stage}.`
      : 'Start with discovery before any judgment or answer-writing.';

    switch (stage.stage) {
      case 'discover':
        return {
          stage: stage.stage,
          executionMode: stage.executionMode,
          prerequisite,
          guidance: 'Parallel retrieval is allowed only for evidence gathering; narrow to candidates before moving on.',
        };
      case 'assess_sources':
        return {
          stage: stage.stage,
          executionMode: stage.executionMode,
          prerequisite: 'Complete discovery and narrow candidate sources first.',
          guidance: 'Audit sources and choose acceptable candidates before query planning. Do not plan the query in parallel with source auditing.',
        };
      case 'plan_query':
        return {
          stage: stage.stage,
          executionMode: stage.executionMode,
          prerequisite: 'Start only after source assessment has identified acceptable sources and scope.',
          guidance: 'Plan against approved sources only. If source suitability is still ambiguous, backtrack instead of improvising a query plan.',
        };
      case 'execute_verify':
        return {
          stage: stage.stage,
          executionMode: stage.executionMode,
          prerequisite: 'Start only after query planning is complete.',
          guidance: 'Run the minimum viable checks and samples. If verification exposes a source or plan issue, backtrack rather than pushing through.',
        };
      case 'render':
        return {
          stage: stage.stage,
          executionMode: stage.executionMode,
          prerequisite: 'Render only after verification has passed or after caveated abstention is decided.',
          guidance: 'Summarize the verified result concisely. Do not reopen discovery, planning, or execution from the render step.',
        };
      default:
        return {
          stage: stage.stage,
          executionMode: stage.executionMode,
          prerequisite,
          guidance: 'Use the stage output from the prior step before proceeding.',
        };
    }
  });
}

export function renderAnalyticalStageExecutionPolicy(
  analyticalPlan?: AnalyticalPlan | null
): string {
  const rules = getDefaultAnalyticalStageExecutionRules(analyticalPlan);
  const rendered = rules.length
    ? rules.map((rule) => (
      `- ${rule.stage} [${rule.executionMode}]\n`
      + `  - prerequisite: ${rule.prerequisite}\n`
      + `  - guidance: ${rule.guidance}`
    )).join('\n')
    : '- discover [parallel_retrieval_only]\n'
      + '  - prerequisite: start with discovery before judgment\n'
      + '  - guidance: only discovery may parallelize retrieval\n'
      + '- assess_sources [serialized]\n'
      + '  - prerequisite: complete discovery first\n'
      + '  - guidance: do not audit sources and plan queries in parallel\n'
      + '- plan_query [serialized]\n'
      + '  - prerequisite: start only after sources are accepted\n'
      + '  - guidance: plan against approved sources only\n'
      + '- execute_verify [serialized]\n'
      + '  - prerequisite: start only after planning is complete\n'
      + '  - guidance: backtrack if verification fails\n'
      + '- render [serialized]\n'
      + '  - prerequisite: render only after verification or abstention\n'
      + '  - guidance: do not reopen earlier stages';

  return `Stage execution policy:
${rendered}

Soft-failure rule:
- if a stage lacks prerequisites, backtrack or ask a clarifying question instead of failing the entire run
- treat these as guidance for reliable sequencing, not as a reason to abandon a recoverable thread`;
}
