/**
 * Canonical system skills shared across desktop sidecar and API.
 *
 * These are extracted from the core system prompt and represented as
 * constants so prompt composition can include only metadata (TOC) by default.
 */

import { SYSTEM_IMPORT_SKILL } from "./system-skill-import.js";
import { SYSTEM_NILE_SKILL_CREATOR_SKILL } from "./system-skill-nile-skill-creator.js";
import { SYSTEM_SAT_SKILL } from "./system-skill-sat.js";
import { SYSTEM_VISUALS_SKILL } from "./system-skill-visuals.js";
import {
  FORMAT_IMPORT_SKILLS,
  UNIVERSAL_SKILLS,
  LOCAL_SKILLS,
  WEB_IMPORT_SKILLS,
} from "./skills/index.js";
import type { PromptMode } from "./prompts.js";

export interface SkillBundledFile {
  path: string;
  content: string;
}

export interface SystemAgentDefinition {
  name: string;
  content: string;
}

export interface SkillDefinition {
  name: string;
  description: string;
  content: string;
  modes?: PromptMode[];  // undefined = available in both modes
  files?: SkillBundledFile[];
  agents?: SystemAgentDefinition[];
}

export interface SystemArtifactFile {
  relativePath: string;
  content: string;
}

export interface SkillMetadata {
  name: string;
  description: string;
}

/** Core system skills (original 3) */
export const CORE_SYSTEM_SKILLS: SkillDefinition[] = [
  SYSTEM_VISUALS_SKILL,
  SYSTEM_IMPORT_SKILL,
  SYSTEM_SAT_SKILL,
  SYSTEM_NILE_SKILL_CREATOR_SKILL,
];

/** All system skills: core + format imports + universal + local + web */
export const SYSTEM_SKILLS: SkillDefinition[] = [
  ...CORE_SYSTEM_SKILLS,
  ...FORMAT_IMPORT_SKILLS,
  ...UNIVERSAL_SKILLS,
  ...LOCAL_SKILLS,
  ...WEB_IMPORT_SKILLS,
];

export const SYSTEM_SKILL_NAMES = SYSTEM_SKILLS.map((skill) => skill.name);

function escapeFrontmatter(value: string): string {
  return value.replace(/\r?\n/g, " ").trim();
}

export function toSkillMetadata(skills: SkillDefinition[]): SkillMetadata[] {
  return skills.map(({ name, description }) => ({ name, description }));
}

export function renderSystemSkillFile(skill: SkillDefinition): string {
  const body = skill.content.trim();
  return `---\nname: ${skill.name}\ndescription: ${escapeFrontmatter(skill.description)}\nsystemManaged: true\n---\n\n${body}\n`;
}

export function buildSystemSkillExtraFiles(skills: SkillDefinition[] = SYSTEM_SKILLS): SystemArtifactFile[] {
  const byPath = new Map<string, string>();

  for (const skill of skills) {
    for (const file of skill.files || []) {
      byPath.set(`.claude/skills/${skill.name}/${file.path}`, file.content);
    }

    for (const agent of skill.agents || []) {
      byPath.set(`.claude/agents/${agent.name}.md`, agent.content);
    }
  }

  return Array.from(byPath.entries())
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([relativePath, content]) => ({ relativePath, content }));
}

export function buildSkillsToc(skills: SkillMetadata[], title = "## Available Skills"): string {
  const rows = skills
    .map((skill) => `- **${skill.name}**: ${skill.description}`)
    .join("\n");

  if (!rows) {
    return "";
  }

  return `${title}\n\n${rows}`;
}

/**
 * Build TOC for system skills, optionally filtered by mode.
 * When mode is provided, only skills that match the mode (or have no mode restriction) are included.
 */
export function buildSystemSkillsToc(title = "## System Skills", mode?: PromptMode): string {
  const filtered = mode
    ? SYSTEM_SKILLS.filter(s => !s.modes || s.modes.includes(mode))
    : SYSTEM_SKILLS;
  return buildSkillsToc(toSkillMetadata(filtered), title);
}

export function isSystemSkillName(name: string): boolean {
  return SYSTEM_SKILL_NAMES.includes(name);
}
