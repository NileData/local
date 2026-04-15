/**
 * POST /chat/invoke-streaming - Streaming local LLM chat via Ollama
 *
 * Streaming local LLM chat handler
 * but targets Ollama's Anthropic-compatible /v1/messages endpoint.
 *
 * Converts Ollama SSE to NDJSON for the client's StreamingAccumulator.
 * Tools and system prompt are loaded from TypeSpec-generated schemas
 * (same source as the non-streaming handler, sorted for KV cache stability).
 */

import type { RequestHandler } from 'express';
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { homedir } from 'node:os';
import { AI_TOOLS } from '../../types/ai-tool-schemas.js';
import { composeAssistantSystemPrompt, getAskUserQuestionToolSchema, type PromptMode, type SkillMetadata } from '../../lib/shared-config/index.js';

// ============================================================================
// Tool & Prompt Loading
// ============================================================================

let cachedTools: Array<{ name: string; description: string; input_schema: unknown }> | null = null;
let cachedSystemPrompt: string | null = null;
let lastSkillsScanMs = 0;
const SKILLS_DIR = join(homedir(), '.nile', 'skills');
const SKILLS_SCAN_INTERVAL_MS = 30_000;

function getLocalTools(): Array<{ name: string; description: string; input_schema: unknown }> {
  if (cachedTools) return cachedTools;
  cachedTools = AI_TOOLS
    .filter((t) => t.tier === 'mvp' && (!t.modes || t.modes.includes('local') || t.modes.includes('web')))
    .sort((a, b) => a.name.localeCompare(b.name))
    .map((t) => ({ name: t.name, description: t.description, input_schema: t.inputSchema.json }));

  const askTool = getAskUserQuestionToolSchema();
  cachedTools.push({
    name: askTool.name,
    description: askTool.description,
    input_schema: askTool.inputSchema.json,
  });

  return cachedTools;
}

async function loadUserSkills(): Promise<SkillMetadata[]> {
  try {
    const entries = await readdir(SKILLS_DIR, { withFileTypes: true });
    const skills: SkillMetadata[] = [];
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      try {
        const content = await readFile(join(SKILLS_DIR, entry.name, 'SKILL.md'), 'utf8');
        const match = content.match(/^---\n([\s\S]*?)\n---/);
        let desc = `Skill: ${entry.name}`;
        if (match) {
          for (const line of match[1].split('\n')) {
            const idx = line.indexOf(':');
            if (idx > 0 && line.substring(0, idx).trim() === 'description') {
              const val = line.substring(idx + 1).trim();
              if (val) { desc = val; break; }
            }
          }
        }
        skills.push({ name: entry.name, description: desc });
      } catch { /* skip */ }
    }
    return skills.sort((a, b) => a.name.localeCompare(b.name));
  } catch { return []; }
}

async function getLocalSystemPrompt(): Promise<string> {
  const now = Date.now();
  if (cachedSystemPrompt && now - lastSkillsScanMs < SKILLS_SCAN_INTERVAL_MS) {
    return cachedSystemPrompt;
  }
  const userSkills = await loadUserSkills();
  cachedSystemPrompt = composeAssistantSystemPrompt({
    chatPromptOptions: { mode: 'local' as PromptMode },
    userSkills: userSkills.length > 0 ? userSkills : undefined,
  });
  lastSkillsScanMs = now;
  return cachedSystemPrompt;
}

// ============================================================================
// Types
// ============================================================================

interface ChatInvokeBody {
  messages: Array<{ role: string; content: unknown }>;
  tools?: Array<{ name: string; description: string; input_schema: unknown }>;
  systemPrompt?: string;
  modelId?: string;
}

// ============================================================================
// Handler
// ============================================================================

export function createChatInvokeStreamingHandler(
  getOllamaPort: () => number,
  localModel: string | (() => Promise<string | null>),
): RequestHandler {
  return async (req, res) => {
    try {
      const body = req.body as ChatInvokeBody;

      let messages: Array<{ role: string; content: unknown }>;
      if (body.messages && Array.isArray(body.messages)) {
        messages = body.messages;
      } else if ('message' in body && typeof (body as { message?: unknown }).message === 'string') {
        messages = [{ role: 'user', content: (body as { message: string }).message }];
      } else {
        res.status(400).json({ error: 'Request must include "messages" array or "message" string' });
        return;
      }

      const tools = body.tools ?? getLocalTools();
      const systemPrompt = body.systemPrompt ?? await getLocalSystemPrompt();
      const resolvedModel = typeof localModel === 'function' ? await localModel() : localModel;
      const model = body.modelId || resolvedModel;
      const ollamaPort = getOllamaPort();
      if (!model) {
        res.write(JSON.stringify({ type: 'error', error: 'No model loaded. Open Settings > AI Assistant and run a model first.' }) + '\n');
        res.end();
        return;
      }

      // Set NDJSON streaming headers
      res.setHeader('Content-Type', 'application/x-ndjson');
      res.setHeader('Transfer-Encoding', 'chunked');
      res.setHeader('Cache-Control', 'no-cache');

      const upstream = await fetch(`http://localhost:${ollamaPort}/v1/messages`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': 'ollama',
          'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify({
          model,
          max_tokens: 4096,
          stream: true,
          system: systemPrompt,
          messages,
          ...(tools.length > 0 && { tools }),
        }),
      });

      if (!upstream.ok || !upstream.body) {
        const errorText = await upstream.text().catch(() => '');
        console.error(`[chat/invoke-streaming] Ollama error ${upstream.status}: ${errorText}`);
        res.write(JSON.stringify({ type: 'error', error: `Ollama ${upstream.status}: ${errorText}` }) + '\n');
        res.end();
        return;
      }

      // Pipe Ollama SSE → NDJSON
      // Ollama's Anthropic-compat streaming sends: "event: ...\ndata: {...}\n\n"
      // Client expects one JSON per line (no "data: " prefix)
      const reader = upstream.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          const trimmed = line.trim();
          if (trimmed.startsWith('data: ')) {
            const jsonStr = trimmed.slice(6);
            if (jsonStr === '[DONE]') continue;
            try {
              JSON.parse(jsonStr); // Validate JSON
              res.write(jsonStr + '\n');
            } catch { /* skip malformed lines */ }
          }
        }
      }

      res.end();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error('[chat/invoke-streaming] Error:', message);

      if (message.includes('ECONNREFUSED') || message.includes('fetch failed')) {
        res.write(JSON.stringify({ type: 'error', error: `Cannot reach Ollama on port ${getOllamaPort()}` }) + '\n');
      } else {
        res.write(JSON.stringify({ type: 'error', error: message }) + '\n');
      }
      res.end();
    }
  };
}
