/**
 * Memory handlers - local filesystem equivalent of cloud S3-backed memory.
 * Stores memory as ~/.nile/memory/MEMORY.md
 */

import type { RequestHandler } from 'express';
import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { invalidateSystemPromptCache } from '../chat/invoke.js';
import { homedir } from 'node:os';
import type {
  Memory,
  GetMemoryResponse,
  UpdateMemoryRequest,
  UpdateMemoryResponse,
} from '../../types/types.js';

const MEMORY_FILE = join(homedir(), '.nile', 'memory', 'MEMORY.md');

export function createMemoryHandlers() {
  const getMemory: RequestHandler = async (_req, res) => {
    try {
      let content = '';
      try {
        content = await readFile(MEMORY_FILE, 'utf8');
      } catch {
        // Empty memory is valid -- file doesn't exist yet
      }

      const memory: Memory = {
        content,
        updatedAt: new Date().toISOString(),
      };

      const response: GetMemoryResponse = { memory };
      res.json(response);
    } catch (err) {
      console.error('[memory] Get error:', err);
      res.status(500).json({ error: 'Failed to get memory' });
    }
  };

  const updateMemory: RequestHandler = async (req, res) => {
    try {
      const body = req.body as UpdateMemoryRequest;
      const content = body.content ?? '';

      await mkdir(dirname(MEMORY_FILE), { recursive: true });
      await writeFile(MEMORY_FILE, content, 'utf8');

      const memory: Memory = {
        content,
        updatedAt: new Date().toISOString(),
      };

      const response: UpdateMemoryResponse = { memory, message: 'Memory updated' };
      invalidateSystemPromptCache(); // Memory content may be used in prompt composition
      res.json(response);
    } catch (err) {
      console.error('[memory] Update error:', err);
      res.status(500).json({ error: 'Failed to update memory' });
    }
  };

  return { getMemory, updateMemory };
}
