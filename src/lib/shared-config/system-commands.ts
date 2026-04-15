export interface CommandDefinition {
  name: string;
  content: string;
}

export const SYSTEM_COMMANDS: CommandDefinition[] = [
  {
    name: "visualize",
    content: `Use the visuals system skill for the current task.

- Prefer the latest relevant query result or structured data already in context.
- Choose a chart or visual summary that matches the user's stated goal.
- Ask one short clarifying question only when the visualization target is underspecified.`,
  },
  {
    name: "import-data",
    content: `Use the import system skill for the current task.

- Identify the source system or file first.
- Confirm any missing destination or schema requirements before proceeding.
- Surface validation, permissions, or connectivity blockers with concrete next steps.`,
  },
  {
    name: "save-as-table",
    content: `Use the sat system skill for the current task.

- Reuse the latest suitable query result when possible.
- Keep naming and schedule choices simple unless the user asks for customization.
- Explain any lineage, refresh, or overwrite implications before applying changes.`,
  },
];

export const SYSTEM_COMMAND_NAMES = SYSTEM_COMMANDS.map((command) => command.name);

export function isSystemCommandName(name: string): boolean {
  return SYSTEM_COMMAND_NAMES.includes(name);
}

export function renderSystemCommandFile(command: CommandDefinition): string {
  return `${command.content.trim()}\n`;
}
