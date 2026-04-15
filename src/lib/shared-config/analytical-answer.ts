export interface AnalyticalAnswerContract {
  requiredFields: Array<'answer' | 'supporting_evidence' | 'assumptions' | 'caveats' | 'verification_status'>;
  concisionRules: string[];
  wordingRules: string[];
}

export function getDefaultAnalyticalAnswerContract(): AnalyticalAnswerContract {
  return {
    requiredFields: ['answer', 'supporting_evidence', 'assumptions', 'caveats', 'verification_status'],
    concisionRules: [
      'lead with the answer rather than setup',
      'avoid repeating the same evidence in multiple sections',
      'keep caveats short and specific',
    ],
    wordingRules: [
      'state observed facts separately from modeled or hypothetical claims',
      'name the comparison group when a comparison is part of the answer',
      'abstain or caveat explicitly when evidence is insufficient',
    ],
  };
}

export function renderAnalyticalAnswerContract(contract: AnalyticalAnswerContract): string {
  const requiredFields = contract.requiredFields.map((field) => `- ${field}`).join('\n');
  const concisionRules = contract.concisionRules.map((rule) => `- ${rule}`).join('\n');
  const wordingRules = contract.wordingRules.map((rule) => `- ${rule}`).join('\n');

  return `Final answer contract:
- required fields:
${requiredFields}
- concision rules:
${concisionRules}
- wording rules:
${wordingRules}`;
}
