/**
 * MCP Integration Registry
 *
 * Defines available MCP integrations for the desktop app.
 * Each integration specifies its auth type, npm package, and default state.
 *
 * Credential trust model:
 * - Nile does NOT store passwords or tokens
 * - env: Credentials read from host environment variables
 * - oauth: Browser-based OAuth flow, tokens stored in OS keychain by the MCP server
 * - api_key: User pastes key in chat (one-time), MCP server handles storage
 * - none: No authentication required
 */

export type McpAuthType = 'env' | 'oauth' | 'api_key' | 'none';

export interface McpIntegration {
  /** Unique identifier (used in config.toml and SDK registration) */
  id: string;
  /** Display name */
  name: string;
  /** Short description */
  description: string;
  /** npm package to install */
  package: string;
  /** Authentication method */
  authType: McpAuthType;
  /** Environment variables needed (for authType: 'env') */
  envVars?: string[];
  /** Enabled by default in fresh installs */
  defaultEnabled: boolean;
  /** Command to start the MCP server (after npm install) */
  command: string;
  /** Arguments for the command */
  args?: string[];
}

/**
 * Built-in MCP integrations available out of the box.
 * Users can also add custom MCP servers via the Settings UI.
 */
export const MCP_INTEGRATIONS: McpIntegration[] = [
  {
    id: 'notion',
    name: 'Notion',
    description: 'Search and read Notion pages, databases, and comments',
    package: '@notionhq/notion-mcp-server',
    authType: 'env',
    envVars: ['NOTION_TOKEN'],
    defaultEnabled: false,
    command: 'npx',
    args: ['-y', '@notionhq/notion-mcp-server'],
  },
  {
    id: 'slack',
    name: 'Slack',
    description: 'Read Slack channels, messages, and threads',
    package: '@modelcontextprotocol/server-slack',
    authType: 'env',
    envVars: ['SLACK_BOT_TOKEN', 'SLACK_TEAM_ID'],
    defaultEnabled: false,
    command: 'npx',
    args: ['-y', '@modelcontextprotocol/server-slack'],
  },
  {
    id: 'github',
    name: 'GitHub',
    description: 'Access GitHub repos, issues, and pull requests',
    package: '@modelcontextprotocol/server-github',
    authType: 'env',
    envVars: ['GITHUB_PERSONAL_ACCESS_TOKEN'],
    defaultEnabled: false,
    command: 'npx',
    args: ['-y', '@modelcontextprotocol/server-github'],
  },
  {
    id: 'postgres',
    name: 'PostgreSQL',
    description: 'Query PostgreSQL databases directly',
    package: '@modelcontextprotocol/server-postgres',
    authType: 'env',
    envVars: ['POSTGRES_URL'],
    defaultEnabled: false,
    command: 'npx',
    args: ['-y', '@modelcontextprotocol/server-postgres'],
  },
  {
    id: 'filesystem',
    name: 'Filesystem',
    description: 'Read and write local files outside the data lake',
    package: '@modelcontextprotocol/server-filesystem',
    authType: 'none',
    defaultEnabled: false,
    command: 'npx',
    args: ['-y', '@modelcontextprotocol/server-filesystem'],
  },
];

/** Custom MCP server definition (user-configured via Settings UI) */
export interface CustomMcpServer {
  /** User-provided name */
  name: string;
  /** Command to start the server */
  command: string;
  /** Arguments */
  args?: string[];
  /** Environment variables to set */
  env?: Record<string, string>;
  /** Whether this custom server is currently enabled */
  enabled: boolean;
}

/** Get integration by ID */
export function getIntegration(id: string): McpIntegration | undefined {
  return MCP_INTEGRATIONS.find(i => i.id === id);
}

/** Get all default-enabled integrations */
export function getDefaultEnabledIntegrations(): McpIntegration[] {
  return MCP_INTEGRATIONS.filter(i => i.defaultEnabled);
}
