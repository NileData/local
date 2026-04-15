/**
 * Skills CRUD handlers - local filesystem equivalent of cloud S3-backed skills.
 * Stores skills as ~/.nile/skills/{name}/SKILL.md
 */

import type { RequestHandler } from 'express';
import { readdir, readFile, writeFile, mkdir, rm, stat } from 'node:fs/promises';
import { join } from 'node:path';
import { invalidateSystemPromptCache } from '../chat/invoke.js';
import { homedir } from 'node:os';
import type {
  Skill,
  ListSkillsResponse,
  GetSkillResponse,
  CreateSkillRequest,
  CreateSkillResponse,
  UpdateSkillRequest,
  UpdateSkillResponse,
  DeleteSkillResponse,
} from '../../types/types.js';

const SKILLS_DIR = join(homedir(), '.nile', 'skills');

/**
 * Extract description from skill markdown content.
 * Checks YAML frontmatter first, then falls back to first heading/line.
 */
function extractDescription(content: string, fallback: string): string {
  const fmMatch = content.match(/^---\n([\s\S]*?)\n---/);
  if (fmMatch) {
    for (const line of fmMatch[1].split('\n')) {
      const idx = line.indexOf(':');
      if (idx > 0 && line.substring(0, idx).trim() === 'description') {
        return line.substring(idx + 1).trim();
      }
    }
  }
  const firstLine = content.split('\n').find((l) => l.trim());
  return firstLine?.replace(/^#+\s*/, '') || `Skill: ${fallback}`;
}

/**
 * Read a single skill from disk. Tries {name}/SKILL.md first, then {name}.md.
 */
async function readSkillFromDisk(name: string): Promise<Skill | null> {
  const candidates = [
    join(SKILLS_DIR, name, 'SKILL.md'),
    join(SKILLS_DIR, `${name}.md`),
  ];

  for (const filePath of candidates) {
    try {
      const content = await readFile(filePath, 'utf8');
      const fileStat = await stat(filePath);
      const description = extractDescription(content, name);
      return {
        name,
        description,
        content,
        createdAt: fileStat.birthtime.toISOString(),
        updatedAt: fileStat.mtime.toISOString(),
      };
    } catch {
      // Not found at this path, try next
    }
  }
  return null;
}

export function createSkillHandlers() {
  const listSkills: RequestHandler = async (_req, res) => {
    try {
      await mkdir(SKILLS_DIR, { recursive: true });
      const entries = await readdir(SKILLS_DIR, { withFileTypes: true });

      const skills: Skill[] = [];
      for (const entry of entries) {
        let name: string;
        if (entry.isDirectory()) {
          name = entry.name;
        } else if (entry.isFile() && entry.name.endsWith('.md')) {
          name = entry.name.replace(/\.md$/, '');
        } else {
          continue;
        }

        const skill = await readSkillFromDisk(name);
        if (skill) {
          skills.push(skill);
        }
      }

      const response: ListSkillsResponse = { skills };
      res.json(response);
    } catch (err) {
      console.error('[skills] List error:', err);
      res.status(500).json({ error: 'Failed to list skills' });
    }
  };

  const getSkill: RequestHandler = async (req, res) => {
    try {
      const name = req.params['name'];
      if (!name) {
        res.status(400).json({ error: 'Skill name is required' });
        return;
      }

      const skill = await readSkillFromDisk(name);
      if (!skill) {
        res.status(404).json({ error: `Skill "${name}" not found` });
        return;
      }

      const response: GetSkillResponse = { skill };
      res.json(response);
    } catch (err) {
      console.error('[skills] Get error:', err);
      res.status(500).json({ error: 'Failed to get skill' });
    }
  };

  const createSkill: RequestHandler = async (req, res) => {
    try {
      const body = req.body as CreateSkillRequest;
      if (!body.name || !body.content) {
        res.status(400).json({ error: 'name and content are required' });
        return;
      }

      const skillDir = join(SKILLS_DIR, body.name);
      await mkdir(skillDir, { recursive: true });
      const filePath = join(skillDir, 'SKILL.md');
      await writeFile(filePath, body.content, 'utf8');

      const skill = await readSkillFromDisk(body.name);
      if (!skill) {
        res.status(500).json({ error: 'Failed to read back created skill' });
        return;
      }
      // Override description from request if provided
      if (body.description) {
        skill.description = body.description;
      }

      const response: CreateSkillResponse = { skill, message: `Skill "${body.name}" created` };
      invalidateSystemPromptCache(); // LLM picks up new skill in next conversation
      res.status(201).json(response);
    } catch (err) {
      console.error('[skills] Create error:', err);
      res.status(500).json({ error: 'Failed to create skill' });
    }
  };

  const updateSkill: RequestHandler = async (req, res) => {
    try {
      const name = req.params['name'];
      if (!name) {
        res.status(400).json({ error: 'Skill name is required' });
        return;
      }

      const body = req.body as UpdateSkillRequest;
      if (!body.content) {
        res.status(400).json({ error: 'content is required' });
        return;
      }

      // Check skill exists
      const existing = await readSkillFromDisk(name);
      if (!existing) {
        res.status(404).json({ error: `Skill "${name}" not found` });
        return;
      }

      const skillDir = join(SKILLS_DIR, name);
      await mkdir(skillDir, { recursive: true });
      await writeFile(join(skillDir, 'SKILL.md'), body.content, 'utf8');

      const skill = await readSkillFromDisk(name);
      if (!skill) {
        res.status(500).json({ error: 'Failed to read back updated skill' });
        return;
      }
      if (body.description) {
        skill.description = body.description;
      }

      const response: UpdateSkillResponse = { skill, message: `Skill "${name}" updated` };
      invalidateSystemPromptCache();
      res.json(response);
    } catch (err) {
      console.error('[skills] Update error:', err);
      res.status(500).json({ error: 'Failed to update skill' });
    }
  };

  const deleteSkill: RequestHandler = async (req, res) => {
    try {
      const name = req.params['name'];
      if (!name) {
        res.status(400).json({ error: 'Skill name is required' });
        return;
      }

      // Try directory first, then single file
      const dirPath = join(SKILLS_DIR, name);
      const filePath = join(SKILLS_DIR, `${name}.md`);
      let deleted = false;

      try {
        await rm(dirPath, { recursive: true });
        deleted = true;
      } catch {
        // Directory doesn't exist
      }

      if (!deleted) {
        try {
          await rm(filePath);
          deleted = true;
        } catch {
          // File doesn't exist either
        }
      }

      if (!deleted) {
        res.status(404).json({ error: `Skill "${name}" not found` });
        return;
      }

      const response: DeleteSkillResponse = { success: true, message: `Skill "${name}" deleted` };
      invalidateSystemPromptCache();
      res.json(response);
    } catch (err) {
      console.error('[skills] Delete error:', err);
      res.status(500).json({ error: 'Failed to delete skill' });
    }
  };

  return { listSkills, getSkill, createSkill, updateSkill, deleteSkill };
}
