/**
 * POST /chat/invoke - Local LLM chat via Ollama
 *
 * Local LLM chat handler targeting Ollama's Anthropic-compatible
 * /v1/messages endpoint.
 *
 * Key differences from cloud:
 * - Uses Ollama instead of Bedrock
 * - Loads tools from TypeSpec-generated schemas (same source, filtered for local mode)
 * - Builds system prompt using shared-config (same as sidecar/cloud)
 * - Tools are sorted by name to ensure stable prefix for Ollama KV cache
 */

import type { RequestHandler } from 'express';
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { homedir } from 'node:os';
import { AI_TOOLS } from '../../types/ai-tool-schemas.js';
import { composeAssistantSystemPrompt, getAskUserQuestionToolSchema, type PromptMode, type SkillMetadata } from '../../lib/shared-config/index.js';

// ============================================================================
// Types
// ============================================================================

interface ChatInvokeBody {
  messages: Array<{ role: string; content: unknown }>;
  tools?: Array<{ name: string; description: string; input_schema: unknown }>;
  systemPrompt?: string;
  modelId?: string;
  executionMode?: string;
}

interface OllamaAnthropicResponse {
  id: string;
  type: string;
  role: string;
  content: Array<{ type: string; text?: string; id?: string; name?: string; input?: Record<string, unknown> }>;
  model: string;
  stop_reason: string;
  usage?: { input_tokens?: number; output_tokens?: number };
}

// ============================================================================
// Tool Loading (cached, sorted for stable Ollama KV cache prefix)
// ============================================================================

let cachedTools: Array<{ name: string; description: string; input_schema: unknown }> | null = null;

function getLocalTools(): Array<{ name: string; description: string; input_schema: unknown }> {
  if (cachedTools) return cachedTools;

  // Filter by MVP tier and local mode (cloud-only tools excluded via TypeSpec @aiTool modes)
  // Sort by name for stable serialization -- prevents Ollama KV cache misses
  const mvpTools = AI_TOOLS
    .filter((t) => t.tier === 'mvp' && (!t.modes || t.modes.includes('local') || t.modes.includes('web')))
    .sort((a, b) => a.name.localeCompare(b.name));

  cachedTools = mvpTools.map((t) => ({
    name: t.name,
    description: t.description,
    input_schema: t.inputSchema.json,
  }));

  // Add AskUserQuestion tool (LLM uses this to ask clarifying questions)
  const askTool = getAskUserQuestionToolSchema();
  cachedTools.push({
    name: askTool.name,
    description: askTool.description,
    input_schema: askTool.inputSchema.json,
  });

  console.log(`[chat/invoke] Loaded ${cachedTools.length} tools for local mode (sorted)`);
  return cachedTools;
}

// ============================================================================
// System Prompt (rebuilds when skills change for accuracy, but tools stay
// stable for KV cache -- the prefix up to the skills section is identical)
// ============================================================================

const SKILLS_DIR = join(homedir(), '.nile', 'skills');
let cachedSystemPrompt: string | null = null;
let lastSkillsScanMs = 0;
const SKILLS_SCAN_INTERVAL_MS = 30_000; // Re-scan skills every 30s

function extractDescription(content: string, fallback: string): string {
  const match = content.match(/^---\n([\s\S]*?)\n---/);
  if (match) {
    for (const line of match[1].split('\n')) {
      const idx = line.indexOf(':');
      if (idx > 0 && line.substring(0, idx).trim() === 'description') {
        const val = line.substring(idx + 1).trim();
        if (val) return val;
      }
    }
  }
  const firstLine = content.split('\n').find(l => l.trim());
  return firstLine?.replace(/^#+\s*/, '') || `Skill: ${fallback}`;
}

async function loadUserSkills(): Promise<SkillMetadata[]> {
  try {
    const entries = await readdir(SKILLS_DIR, { withFileTypes: true });
    const skills: SkillMetadata[] = [];
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const skillPath = join(SKILLS_DIR, entry.name, 'SKILL.md');
      try {
        const content = await readFile(skillPath, 'utf8');
        skills.push({ name: entry.name, description: extractDescription(content, entry.name) });
      } catch { /* skill dir without SKILL.md -- skip */ }
    }
    skills.sort((a, b) => a.name.localeCompare(b.name));
    return skills;
  } catch {
    return []; // ~/.nile/skills/ doesn't exist yet
  }
}

async function getLocalSystemPrompt(): Promise<string> {
  const now = Date.now();
  if (cachedSystemPrompt && now - lastSkillsScanMs < SKILLS_SCAN_INTERVAL_MS) {
    return cachedSystemPrompt;
  }

  const mode: PromptMode = 'local';
  const userSkills = await loadUserSkills();
  cachedSystemPrompt = composeAssistantSystemPrompt({
    chatPromptOptions: { mode },
    userSkills: userSkills.length > 0 ? userSkills : undefined,
  });
  lastSkillsScanMs = now;

  console.log(`[chat/invoke] Built system prompt: ${cachedSystemPrompt.length} chars, ${userSkills.length} user skills`);
  return cachedSystemPrompt;
}

/** Invalidate cached system prompt (call after skill/command/memory CRUD) */
export function invalidateSystemPromptCache(): void {
  cachedSystemPrompt = null;
  lastSkillsScanMs = 0;
}

// ============================================================================
// Handler
// ============================================================================

export function createChatInvokeHandler(
  getOllamaPort: () => number,
  localModel: string | (() => Promise<string | null>),
): RequestHandler {
  return async (req, res) => {
    try {
      const body = req.body as ChatInvokeBody;

      // Support both { messages: [...] } and { message: "text" } formats
      let messages: Array<{ role: string; content: unknown }>;
      if (body.messages && Array.isArray(body.messages)) {
        messages = body.messages;
      } else if ('message' in body && typeof (body as { message?: unknown }).message === 'string') {
        messages = [{ role: 'user', content: (body as { message: string }).message }];
      } else {
        res.status(400).json({ error: 'Request must include "messages" array or "message" string' });
        return;
      }

      // Use client-provided tools/systemPrompt if given, otherwise load defaults
      const tools = body.tools ?? getLocalTools();
      const systemPrompt = body.systemPrompt ?? await getLocalSystemPrompt();
      const resolvedModel = typeof localModel === 'function' ? await localModel() : localModel;
      const model = body.modelId || resolvedModel;
      const ollamaPort = getOllamaPort();
      if (!model) {
        res.status(503).json({ error: 'NO_MODEL_LOADED', message: 'No model loaded. Open Settings > AI Assistant and run a model first.' });
        return;
      }

      const response = await fetch(`http://localhost:${ollamaPort}/v1/messages`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': 'ollama',
          'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify({
          model,
          max_tokens: 4096,
          system: systemPrompt,
          messages,
          ...(tools.length > 0 && { tools }),
        }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`[chat/invoke] Ollama error ${response.status}: ${errorText}`);
        res.status(502).json({
          error: 'OLLAMA_ERROR',
          message: `Ollama returned ${response.status}: ${errorText}`,
        });
        return;
      }

      const result = (await response.json()) as OllamaAnthropicResponse;

      // Map to ChatInvokeResponse format
      res.status(200).json({
        content: result.content ?? [],
        stopReason: result.stop_reason ?? 'end_turn',
        usage: {
          inputTokens: result.usage?.input_tokens ?? 0,
          outputTokens: result.usage?.output_tokens ?? 0,
        },
        modelId: result.model ?? model,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error('[chat/invoke] Error:', message);

      if (message.includes('ECONNREFUSED') || message.includes('fetch failed')) {
        res.status(503).json({
          error: 'OLLAMA_UNAVAILABLE',
          message: `Cannot reach Ollama. Ensure Ollama is running.`,
        });
        return;
      }

      res.status(500).json({ error: 'CHAT_INVOKE_ERROR', message });
    }
  };
}
