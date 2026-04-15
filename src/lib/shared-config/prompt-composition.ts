import { buildChatSystemPrompt, type ChatPromptOptions, type PromptMode } from "./prompts.js";
import { buildSkillsToc, buildSystemSkillsToc, type SkillMetadata } from "./system-skills.js";

export interface PromptCompositionOptions {
  corePrompt?: string;
  memoryContent?: string;
  userSkills?: SkillMetadata[];
  includeSystemSkillsToc?: boolean;
  workspaceContext?: string;
  /** Mode-aware prompt options (passed to buildChatSystemPrompt when no corePrompt override) */
  chatPromptOptions?: ChatPromptOptions;
}

export function formatMemorySection(memoryContent?: string): string {
  const memory = (memoryContent || "").trim();
  if (!memory) {
    return "";
  }

  return `## Memory\n\n${memory}`;
}

export function composeAssistantSystemPrompt(options: PromptCompositionOptions = {}): string {
  const mode: PromptMode | undefined = options.chatPromptOptions?.mode;
  const corePrompt = options.corePrompt || buildChatSystemPrompt(options.chatPromptOptions);
  const memorySection = formatMemorySection(options.memoryContent);
  const systemSkillsToc = options.includeSystemSkillsToc === false ? "" : buildSystemSkillsToc(undefined, mode);
  const userSkillsToc = options.userSkills && options.userSkills.length > 0
    ? buildSkillsToc(options.userSkills)
    : "";
  const workspaceContext = (options.workspaceContext || "").trim();

  return [
    corePrompt.trim(),
    memorySection,
    systemSkillsToc,
    userSkillsToc,
    workspaceContext ? `# WORKSPACE CONTEXT\n\n${workspaceContext}` : "",
  ]
    .filter((part) => part.length > 0)
    .join("\n\n");
}

