/**
 * Shared Configuration Package
 *
 * Single source of truth for configuration shared across:
 * - api (Lambda)
 * - client-app (web)
 * - desktop-app/sidecar (Claude Agent SDK)
 */

// Product branding
export {
  DEFAULT_PRODUCT_NAME,
  DEFAULT_PRODUCT_SHORT_NAME,
  getProductName,
  getProductShortName,
} from './product.js';

// Bedrock model configuration
export {
  DEFAULT_MODEL_ID,
  getModelId,
} from './bedrock.js';

export {
  CHAT_TEMPERATURE,
  ROUTER_TEMPERATURE,
  QUERY_GENERATION_TEMPERATURE,
  INSIGHT_TEMPERATURE,
  VISUAL_TEMPERATURE,
  TITLE_TEMPERATURE,
  SDK_EFFORT,
} from './ai-runtime-policy.js';

export {
  getDefaultAnalyticalAnswerContract,
  renderAnalyticalAnswerContract,
  type AnalyticalAnswerContract,
} from './analytical-answer.js';

export {
  ANALYTICAL_CLARIFICATION_PROMPT,
  ANALYTICAL_SOURCE_ASSESSMENT_PROMPT,
  ANALYTICAL_VERIFIER_PROMPT,
  ANALYTICAL_RENDERER_PROMPT,
  getDefaultAnalyticalRenderedAnswer,
  getDefaultAnalyticalSourceAssessment,
  getDefaultAnalyticalVerificationReport,
  normalizeAnalyticalRenderedAnswer,
  normalizeAnalyticalSourceAssessment,
  normalizeAnalyticalVerificationReport,
  parseAnalyticalClarificationRequest,
  parseAnalyticalJsonObject,
  renderAnalyticalExecutionContextHeader,
  type AnalyticalRecommendedUse,
  type AnalyticalRenderedAnswer,
  type AnalyticalSourceAssessment,
  type AnalyticalSourceAssessmentCandidate,
  type AnalyticalSourceAssessmentRejectedCandidate,
  type AnalyticalVerificationReport,
  type AnalyticalVerificationStatus,
} from './analytical-execution.js';

export {
  ANALYTICAL_ROUTE_PROMPT,
  applyExecutionModePrompt,
  getDefaultAnalyticalRouteDecision,
  normalizeAnalyticalRouteDecision,
  parseAnalyticalRouteDecision,
  type AnalyticalQuestionClass,
  type AnalyticalRouteDecision,
  type ChatExecutionMode,
} from './analytical-routing.js';

export {
  ANALYTICAL_PLANNER_PROMPT,
  getDefaultAnalyticalPlan,
  normalizeAnalyticalPlan,
  parseAnalyticalPlan,
  type AnalyticalDeliverable,
  type AnalyticalPlan,
  type AnalyticalPlanStage,
  type AnalyticalStageExecutionMode,
  type AnalyticalStageFallback,
  type AnalyticalStageName,
  type AnalyticalWorker,
} from './analytical-planning.js';

export {
  getDefaultAnalyticalStageExecutionRules,
  renderAnalyticalStageExecutionPolicy,
  type AnalyticalStageExecutionRule,
} from './analytical-stage-policy.js';

export {
  getSourceSelectionPolicy,
  renderSourceSelectionPolicy,
  type AnalyticalSourcePolicy,
  type AnalyticalSourceRole,
} from './analytical-source-policy.js';

export {
  getDefaultAnalyticalVerificationContract,
  renderAnalyticalVerificationContract,
  type AnalyticalStageResultStatus,
  type AnalyticalVerificationContract,
  type AnalyticalVerificationStageRule,
} from './analytical-verification.js';

export {
  ASK_USER_QUESTION_TOOL_NAME,
  ASK_USER_QUESTION_TOOL_DESCRIPTION,
  getAskUserQuestionToolSchema,
  isClarifyingQuestionRequest,
  type ClarifyingQuestion,
  type ClarifyingQuestionMetadata,
  type ClarifyingQuestionOption,
  type ClarifyingQuestionRequest,
  type ClarifyingQuestionResolution,
  type ClarifyingQuestionResponse,
  type ClarifyingQuestionSkipReason,
  type ClarifyingQuestionSkippedResponse,
  type ClarifyingQuestionToolSchema,
} from './clarifying-questions.js';

// AI system prompts - chat
export {
  getMinimalChatPrompt,
  buildChatSystemPrompt,
  type PromptMode,
  type SystemResourceContext,
  type ChatPromptOptions,
} from './prompts.js';

// AI system prompts - local mode
export {
  buildLocalChatSystemPrompt,
} from './prompts-local.js';

// AI prompt composition helpers and system skills
export {
  SYSTEM_SKILLS,
  CORE_SYSTEM_SKILLS,
  SYSTEM_SKILL_NAMES,
  toSkillMetadata,
  renderSystemSkillFile,
  buildSystemSkillExtraFiles,
  buildSkillsToc,
  buildSystemSkillsToc,
  isSystemSkillName,
  type SkillDefinition,
  type SkillBundledFile,
  type SkillMetadata,
  type SystemAgentDefinition,
  type SystemArtifactFile,
} from './system-skills.js';

// Format-specific and specialized skills
export {
  FORMAT_IMPORT_SKILLS,
  UNIVERSAL_SKILLS,
  LOCAL_SKILLS,
  WEB_IMPORT_SKILLS,
  ALL_EXTRA_SKILLS,
} from './skills/index.js';

export {
  SYSTEM_COMMANDS,
  SYSTEM_COMMAND_NAMES,
  isSystemCommandName,
  renderSystemCommandFile,
  type CommandDefinition,
} from './system-commands.js';

export {
  composeAssistantSystemPrompt,
  formatMemorySection,
  type PromptCompositionOptions,
} from './prompt-composition.js';

// MCP integration registry
export {
  MCP_INTEGRATIONS,
  getIntegration,
  getDefaultEnabledIntegrations,
  type McpAuthType,
  type McpIntegration,
  type CustomMcpServer,
} from './mcp-integrations.js';

export {
  buildAiImportStagingKeyPrefix,
  buildAiImportStagingS3Uri,
  formatAiImportTimestamp,
  sanitizeImportPathSegment,
  type AiImportStagingPathOptions,
  type AiImportStagingS3UriOptions,
} from './import-staging.js';

export {
  buildDvcWorkspaceContext,
  getDefaultAiImportPrefix,
  type DvcWorkspaceContextOptions,
} from './workspace-context.js';

// AI system prompts - specialized (query, insights, visualization)
export {
  AI_QUERY_SYSTEM_PROMPT,
  AI_SESSION_TITLE_PROMPT,
  INSIGHT_CLASSIFICATION_PROMPT,
  INSIGHT_CLASSIFICATION_LITE_PROMPT,
  VISUAL_GENERATION_PROMPT,
} from './prompts-specialized.js';
