export const ASK_USER_QUESTION_TOOL_NAME = 'ask_user_question';
export const ASK_USER_QUESTION_TOOL_DESCRIPTION = 'Ask the user a small number of focused clarifying questions when ambiguity would materially change the answer, query, or tool choice.';

export interface ClarifyingQuestionOption {
  label: string;
  description: string;
}

export interface ClarifyingQuestion {
  question: string;
  header: string;
  options: ClarifyingQuestionOption[];
  multiSelect?: boolean;
}

export interface ClarifyingQuestionMetadata {
  source?: string;
}

export type ClarifyingQuestionSkipReason =
  | 'user_canceled'
  | 'stop'
  | 'session_reset'
  | 'abort'
  | 'timeout'
  | 'unknown';

export interface ClarifyingQuestionRequest {
  questions: ClarifyingQuestion[];
  answers?: Record<string, string>;
  metadata?: ClarifyingQuestionMetadata;
}

export interface ClarifyingQuestionResponse {
  questions: ClarifyingQuestion[];
  answers: Record<string, string>;
  metadata?: ClarifyingQuestionMetadata;
}

export interface ClarifyingQuestionSkippedResponse {
  questions: ClarifyingQuestion[];
  canceled: true;
  skipReason?: ClarifyingQuestionSkipReason;
  metadata?: ClarifyingQuestionMetadata;
}

export type ClarifyingQuestionResolution =
  | {
      kind: 'answered';
      answers: Record<string, string>;
    }
  | {
      kind: 'skipped';
      skipReason: ClarifyingQuestionSkipReason;
    };

export interface ClarifyingQuestionToolSchema {
  name: typeof ASK_USER_QUESTION_TOOL_NAME;
  description: string;
  inputSchema: {
    json: Record<string, unknown>;
  };
}

export function isClarifyingQuestionRequest(value: unknown): value is ClarifyingQuestionRequest {
  if (!value || typeof value !== 'object') {
    return false;
  }

  const candidate = value as ClarifyingQuestionRequest;
  if (!Array.isArray(candidate.questions) || candidate.questions.length === 0 || candidate.questions.length > 4) {
    return false;
  }

  return candidate.questions.every(question => (
    !!question
    && typeof question.question === 'string'
    && question.question.trim().length > 0
    && typeof question.header === 'string'
    && question.header.trim().length > 0
    && Array.isArray(question.options)
    && question.options.length >= 2
    && question.options.length <= 4
    && question.options.every(option => (
      !!option
      && typeof option.label === 'string'
      && option.label.trim().length > 0
      && typeof option.description === 'string'
      && option.description.trim().length > 0
    ))
  ));
}

export function getAskUserQuestionToolSchema(): ClarifyingQuestionToolSchema {
  return {
    name: ASK_USER_QUESTION_TOOL_NAME,
    description: ASK_USER_QUESTION_TOOL_DESCRIPTION,
    inputSchema: {
      json: {
        type: 'object',
        additionalProperties: false,
        properties: {
          questions: {
            type: 'array',
            minItems: 1,
            maxItems: 4,
            items: {
              type: 'object',
              additionalProperties: false,
              properties: {
                question: { type: 'string', minLength: 1 },
                header: { type: 'string', minLength: 1, maxLength: 12 },
                options: {
                  type: 'array',
                  minItems: 2,
                  maxItems: 4,
                  items: {
                    type: 'object',
                    additionalProperties: false,
                    properties: {
                      label: { type: 'string', minLength: 1 },
                      description: { type: 'string', minLength: 1 },
                    },
                    required: ['label', 'description'],
                  },
                },
                multiSelect: { type: 'boolean' },
              },
              required: ['question', 'header', 'options'],
            },
          },
          metadata: {
            type: 'object',
            additionalProperties: false,
            properties: {
              source: { type: 'string' },
            },
          },
        },
        required: ['questions'],
      },
    },
  };
}
