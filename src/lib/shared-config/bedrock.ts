/**
 * Bedrock Configuration
 *
 * Single source of truth for AWS Bedrock model configuration.
 * Used by: api, client-app, desktop-app/sidecar
 */

/**
 * Default Claude model ID for Bedrock
 *
 * Format: {scope}.anthropic.{model-name}
 * - us. prefix = US region cross-region inference
 * - claude-sonnet-4-6 = Claude Sonnet 4.6 inference profile
 */
export const DEFAULT_MODEL_ID = 'us.anthropic.claude-sonnet-4-6';

/**
 * Default Claude model ID for direct Anthropic API
 */
export const DEFAULT_ANTHROPIC_MODEL_ID = 'claude-sonnet-4-5-20250929';

/**
 * Get the model ID from environment or default.
 * Returns Bedrock-format ID when using Bedrock, Anthropic-format when using API key.
 */
export function getModelId(): string {
  if (process.env.CLAUDE_MODEL) return process.env.CLAUDE_MODEL;
  if (process.env.BEDROCK_MODEL_ID) return process.env.BEDROCK_MODEL_ID;

  // Local LLM mode: return the Ollama model name
  if (process.env.NILE_AI_PROVIDER === 'local' && process.env.NILE_LOCAL_MODEL) {
    return process.env.NILE_LOCAL_MODEL;
  }

  // Use Anthropic-format model ID in local mode (direct API key)
  if (process.env.DVC_LOCAL_MODE === '1') {
    return DEFAULT_ANTHROPIC_MODEL_ID;
  }

  return DEFAULT_MODEL_ID;
}
