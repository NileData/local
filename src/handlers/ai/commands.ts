/**
 * Commands CRUD handlers - local filesystem equivalent of cloud S3-backed commands.
 * Stores commands as ~/.nile/commands/{name}.md
 */

import type { RequestHandler } from 'express';
import { readdir, readFile, writeFile, mkdir, rm, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { invalidateSystemPromptCache } from '../chat/invoke.js';
import { homedir } from 'node:os';
import type {
  Command,
  ListCommandsResponse,
  GetCommandResponse,
  CreateCommandRequest,
  CreateCommandResponse,
  UpdateCommandRequest,
  UpdateCommandResponse,
  DeleteCommandResponse,
} from '../../types/types.js';

const COMMANDS_DIR = join(homedir(), '.nile', 'commands');

/**
 * Read a single command from disk.
 */
async function readCommandFromDisk(name: string): Promise<Command | null> {
  const filePath = join(COMMANDS_DIR, `${name}.md`);
  try {
    const content = await readFile(filePath, 'utf8');
    const fileStat = await stat(filePath);
    return {
      name,
      content,
      updatedAt: fileStat.mtime.toISOString(),
    };
  } catch {
    return null;
  }
}

export function createCommandHandlers() {
  const listCommands: RequestHandler = async (_req, res) => {
    try {
      await mkdir(COMMANDS_DIR, { recursive: true });
      const entries = await readdir(COMMANDS_DIR, { withFileTypes: true });

      const commands: Command[] = [];
      for (const entry of entries) {
        if (!entry.isFile() || !entry.name.endsWith('.md')) continue;
        const name = entry.name.replace(/\.md$/, '');
        const command = await readCommandFromDisk(name);
        if (command) {
          commands.push(command);
        }
      }

      const response: ListCommandsResponse = { commands };
      res.json(response);
    } catch (err) {
      console.error('[commands] List error:', err);
      res.status(500).json({ error: 'Failed to list commands' });
    }
  };

  const getCommand: RequestHandler = async (req, res) => {
    try {
      const name = req.params['name'];
      if (!name) {
        res.status(400).json({ error: 'Command name is required' });
        return;
      }

      const command = await readCommandFromDisk(name);
      if (!command) {
        res.status(404).json({ error: `Command "${name}" not found` });
        return;
      }

      const response: GetCommandResponse = { command };
      res.json(response);
    } catch (err) {
      console.error('[commands] Get error:', err);
      res.status(500).json({ error: 'Failed to get command' });
    }
  };

  const createCommand: RequestHandler = async (req, res) => {
    try {
      const body = req.body as CreateCommandRequest;
      if (!body.name || !body.content) {
        res.status(400).json({ error: 'name and content are required' });
        return;
      }

      await mkdir(COMMANDS_DIR, { recursive: true });
      const filePath = join(COMMANDS_DIR, `${body.name}.md`);
      await writeFile(filePath, body.content, 'utf8');

      const command = await readCommandFromDisk(body.name);
      if (!command) {
        res.status(500).json({ error: 'Failed to read back created command' });
        return;
      }

      const response: CreateCommandResponse = { command, message: `Command "${body.name}" created` };
      invalidateSystemPromptCache();
      res.status(201).json(response);
    } catch (err) {
      console.error('[commands] Create error:', err);
      res.status(500).json({ error: 'Failed to create command' });
    }
  };

  const updateCommand: RequestHandler = async (req, res) => {
    try {
      const name = req.params['name'];
      if (!name) {
        res.status(400).json({ error: 'Command name is required' });
        return;
      }

      const body = req.body as UpdateCommandRequest;
      if (!body.content) {
        res.status(400).json({ error: 'content is required' });
        return;
      }

      const existing = await readCommandFromDisk(name);
      if (!existing) {
        res.status(404).json({ error: `Command "${name}" not found` });
        return;
      }

      await writeFile(join(COMMANDS_DIR, `${name}.md`), body.content, 'utf8');

      const command = await readCommandFromDisk(name);
      if (!command) {
        res.status(500).json({ error: 'Failed to read back updated command' });
        return;
      }

      const response: UpdateCommandResponse = { command, message: `Command "${name}" updated` };
      invalidateSystemPromptCache();
      res.json(response);
    } catch (err) {
      console.error('[commands] Update error:', err);
      res.status(500).json({ error: 'Failed to update command' });
    }
  };

  const deleteCommand: RequestHandler = async (req, res) => {
    try {
      const name = req.params['name'];
      if (!name) {
        res.status(400).json({ error: 'Command name is required' });
        return;
      }

      const filePath = join(COMMANDS_DIR, `${name}.md`);
      try {
        await rm(filePath);
      } catch {
        res.status(404).json({ error: `Command "${name}" not found` });
        return;
      }

      const response: DeleteCommandResponse = { success: true, message: `Command "${name}" deleted` };
      invalidateSystemPromptCache();
      res.json(response);
    } catch (err) {
      console.error('[commands] Delete error:', err);
      res.status(500).json({ error: 'Failed to delete command' });
    }
  };

  return { listCommands, getCommand, createCommand, updateCommand, deleteCommand };
}
