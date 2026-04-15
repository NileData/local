import { DatabaseSync } from 'node:sqlite';
import { randomUUID } from 'crypto';
import { mkdirSync } from 'fs';
import { dirname } from 'path';
import type { LocalJob } from '../types/local.js';

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS databases (
  database_name TEXT PRIMARY KEY,
  description TEXT,
  team_name TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS tables (
  table_id TEXT PRIMARY KEY,
  database_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  description TEXT,
  schema_json TEXT,
  table_type TEXT NOT NULL DEFAULT 'MANAGED',
  source_path TEXT,
  row_count INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT,
  FOREIGN KEY (database_name) REFERENCES databases(database_name)
);

CREATE TABLE IF NOT EXISTS jobs (
  job_id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'QUEUED',
  engine TEXT NOT NULL DEFAULT 'spark',
  input_json TEXT NOT NULL,
  result_json TEXT,
  result_path TEXT,
  error_message TEXT,
  database_name TEXT,
  table_name TEXT,
  row_count INTEGER,
  submitted_at TEXT NOT NULL,
  completed_at TEXT
);

CREATE TABLE IF NOT EXISTS saved_queries (
  query_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  sql_text TEXT NOT NULL,
  description TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS connectors (
  connector_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  type TEXT NOT NULL,
  config_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS chat_sessions (
  session_id TEXT PRIMARY KEY,
  title TEXT,
  messages_json TEXT NOT NULL DEFAULT '[]',
  created_at TEXT NOT NULL,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL,
  description TEXT,
  metadata_json TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS config (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT
);
`;

export class SQLiteService {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private db: any; // DatabaseSync — node:sqlite types may not be in @types/node yet

  constructor(dbPath: string) {
    mkdirSync(dirname(dbPath), { recursive: true });
    this.db = new DatabaseSync(dbPath);
    this.db.exec('PRAGMA journal_mode = WAL');
    this.initSchema();
  }

  private initSchema(): void {
    this.db.exec(SCHEMA_SQL);
    this.migrateSchema();
  }

  /** Add columns that may be missing from older catalog.db files. */
  private migrateSchema(): void {
    const migrations = [
      { table: 'databases', column: 'team_name', sql: 'ALTER TABLE databases ADD COLUMN team_name TEXT' },
      { table: 'jobs', column: 'database_name', sql: 'ALTER TABLE jobs ADD COLUMN database_name TEXT' },
      { table: 'jobs', column: 'table_name', sql: 'ALTER TABLE jobs ADD COLUMN table_name TEXT' },
      { table: 'jobs', column: 'row_count', sql: 'ALTER TABLE jobs ADD COLUMN row_count INTEGER' },
      { table: 'tables', column: 'table_type', sql: "ALTER TABLE tables ADD COLUMN table_type TEXT NOT NULL DEFAULT 'MANAGED'" },
      { table: 'tables', column: 'source_path', sql: 'ALTER TABLE tables ADD COLUMN source_path TEXT' },
      { table: 'tables', column: 'row_count', sql: 'ALTER TABLE tables ADD COLUMN row_count INTEGER' },
      { table: 'tables', column: 'definition_json', sql: 'ALTER TABLE tables ADD COLUMN definition_json TEXT' },
      { table: 'chat_sessions', column: 'starred', sql: 'ALTER TABLE chat_sessions ADD COLUMN starred INTEGER NOT NULL DEFAULT 0' },
      { table: 'chat_sessions', column: 'compacted_json', sql: "ALTER TABLE chat_sessions ADD COLUMN compacted_json TEXT NOT NULL DEFAULT '[]'" },
      { table: 'chat_sessions', column: 'context_json', sql: "ALTER TABLE chat_sessions ADD COLUMN context_json TEXT NOT NULL DEFAULT '{}'" },
      { table: 'chat_sessions', column: 'user_id', sql: "ALTER TABLE chat_sessions ADD COLUMN user_id TEXT NOT NULL DEFAULT 'local'" },
      { table: 'chat_sessions', column: 'compaction_count', sql: 'ALTER TABLE chat_sessions ADD COLUMN compaction_count INTEGER NOT NULL DEFAULT 0' },
    ];
    for (const m of migrations) {
      const cols = this.db.prepare(`PRAGMA table_info(${m.table})`).all() as Array<{ name: string }>;
      if (!cols.some(c => c.name === m.column)) {
        this.db.exec(m.sql);
      }
    }
  }

  getVersion(): string {
    const row = this.db.prepare('SELECT sqlite_version() AS version').get() as { version: string };
    return row.version;
  }

  // ── Jobs ──────────────────────────────────────────────────────

  insertJob(job: LocalJob): void {
    this.db.prepare(`
      INSERT INTO jobs (job_id, type, status, engine, input_json, result_json, result_path, error_message, database_name, table_name, row_count, submitted_at, completed_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      job.jobId,
      job.type,
      job.status,
      job.engine,
      job.inputJson,
      job.resultJson ?? null,
      job.resultPath ?? null,
      job.errorMessage ?? null,
      job.databaseName ?? null,
      job.tableName ?? null,
      job.rowCount ?? null,
      job.submittedAt,
      job.completedAt ?? null,
    );
  }

  updateJob(jobId: string, updates: Partial<LocalJob>): void {
    const fields: string[] = [];
    const values: unknown[] = [];

    if (updates.status !== undefined) { fields.push('status = ?'); values.push(updates.status); }
    if (updates.resultJson !== undefined) { fields.push('result_json = ?'); values.push(updates.resultJson); }
    if (updates.resultPath !== undefined) { fields.push('result_path = ?'); values.push(updates.resultPath); }
    if (updates.errorMessage !== undefined) { fields.push('error_message = ?'); values.push(updates.errorMessage); }
    if (updates.completedAt !== undefined) { fields.push('completed_at = ?'); values.push(updates.completedAt); }
    if (updates.rowCount !== undefined) { fields.push('row_count = ?'); values.push(updates.rowCount); }

    if (fields.length === 0) return;

    values.push(jobId);
    this.db.prepare(`UPDATE jobs SET ${fields.join(', ')} WHERE job_id = ?`).run(...values);
  }

  getJob(jobId: string): LocalJob | undefined {
    const row = this.db.prepare('SELECT * FROM jobs WHERE job_id = ?').get(jobId) as Record<string, unknown> | undefined;
    if (!row) return undefined;

    return {
      jobId: row['job_id'] as string,
      type: row['type'] as LocalJob['type'],
      status: row['status'] as LocalJob['status'],
      engine: row['engine'] as LocalJob['engine'],
      inputJson: row['input_json'] as string,
      resultJson: (row['result_json'] as string) ?? undefined,
      resultPath: (row['result_path'] as string) ?? undefined,
      errorMessage: (row['error_message'] as string) ?? undefined,
      submittedAt: row['submitted_at'] as string,
      completedAt: (row['completed_at'] as string) ?? undefined,
      databaseName: (row['database_name'] as string) ?? undefined,
      tableName: (row['table_name'] as string) ?? undefined,
      rowCount: (row['row_count'] as number) ?? undefined,
    };
  }

  // ── Database CRUD ──────────────────────────────────────────────

  insertDatabase(name: string, description?: string, teamName?: string): void {
    this.db.prepare(`
      INSERT OR IGNORE INTO databases (database_name, description, team_name, created_at)
      VALUES (?, ?, ?, ?)
    `).run(name, description ?? null, teamName ?? null, new Date().toISOString());
  }

  getDatabase(name: string): { name: string; description?: string; teamName?: string; createdAt: string } | undefined {
    const row = this.db.prepare('SELECT * FROM databases WHERE database_name = ?').get(name) as
      Record<string, unknown> | undefined;
    if (!row) return undefined;
    return {
      name: row['database_name'] as string,
      description: (row['description'] as string) ?? undefined,
      teamName: (row['team_name'] as string) ?? undefined,
      createdAt: row['created_at'] as string,
    };
  }

  deleteDatabase(name: string): void {
    this.db.prepare('DELETE FROM tables WHERE database_name = ?').run(name);
    this.db.prepare('DELETE FROM databases WHERE database_name = ?').run(name);
  }

  listDatabases(teamName?: string): Array<{ name: string; description?: string; teamName?: string; createdAt: string }> {
    const rows = (teamName
      ? this.db.prepare('SELECT * FROM databases WHERE team_name = ?').all(teamName)
      : this.db.prepare('SELECT * FROM databases').all()) as Record<string, unknown>[];
    return rows.map((row) => ({
      name: row['database_name'] as string,
      description: (row['description'] as string) ?? undefined,
      teamName: (row['team_name'] as string) ?? undefined,
      createdAt: row['created_at'] as string,
    }));
  }

  // ── Table CRUD ────────────────────────────────────────────────

  insertTable(
    databaseName: string,
    tableName: string,
    options?: { description?: string; schemaJson?: string; tableType?: string; sourcePath?: string; rowCount?: number; definitionJson?: string },
  ): string {
    const tableId = randomUUID();
    this.db.prepare(`
      INSERT INTO tables (table_id, database_name, table_name, description, schema_json, table_type, source_path, row_count, definition_json, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      tableId,
      databaseName,
      tableName,
      options?.description ?? null,
      options?.schemaJson ?? null,
      options?.tableType ?? 'MANAGED',
      options?.sourcePath ?? null,
      options?.rowCount ?? null,
      options?.definitionJson ?? null,
      new Date().toISOString(),
    );
    return tableId;
  }

  getTable(databaseName: string, tableName: string): { tableId: string; databaseName: string; tableName: string; description?: string; schemaJson?: string; tableType: string; sourcePath?: string; rowCount?: number; definitionJson?: string; createdAt: string; updatedAt?: string } | undefined {
    const row = this.db.prepare('SELECT * FROM tables WHERE database_name = ? AND table_name = ?').get(databaseName, tableName) as
      Record<string, unknown> | undefined;
    if (!row) return undefined;
    return {
      tableId: row['table_id'] as string,
      databaseName: row['database_name'] as string,
      tableName: row['table_name'] as string,
      description: (row['description'] as string) ?? undefined,
      schemaJson: (row['schema_json'] as string) ?? undefined,
      tableType: (row['table_type'] as string) ?? 'MANAGED',
      sourcePath: (row['source_path'] as string) ?? undefined,
      rowCount: (row['row_count'] as number) ?? undefined,
      definitionJson: (row['definition_json'] as string) ?? undefined,
      createdAt: row['created_at'] as string,
      updatedAt: (row['updated_at'] as string) ?? undefined,
    };
  }

  deleteTable(databaseName: string, tableName: string): void {
    this.db.prepare('DELETE FROM tables WHERE database_name = ? AND table_name = ?').run(databaseName, tableName);
  }

  listTables(databaseName?: string): Array<{ tableId: string; databaseName: string; tableName: string; description?: string; schemaJson?: string; tableType: string; sourcePath?: string; rowCount?: number; definitionJson?: string; createdAt: string }> {
    const rows = (databaseName
      ? this.db.prepare('SELECT * FROM tables WHERE database_name = ?').all(databaseName)
      : this.db.prepare('SELECT * FROM tables').all()) as Record<string, unknown>[];
    return rows.map((row) => ({
      tableId: row['table_id'] as string,
      databaseName: row['database_name'] as string,
      tableName: row['table_name'] as string,
      description: (row['description'] as string) ?? undefined,
      schemaJson: (row['schema_json'] as string) ?? undefined,
      tableType: (row['table_type'] as string) ?? 'MANAGED',
      sourcePath: (row['source_path'] as string) ?? undefined,
      rowCount: (row['row_count'] as number) ?? undefined,
      definitionJson: (row['definition_json'] as string) ?? undefined,
      createdAt: row['created_at'] as string,
    }));
  }

  // ── Query history ─────────────────────────────────────────────

  insertQueryHistory(sql: string, databaseName: string | null, rowCount: number): void {
    this.db.prepare(`
      INSERT INTO saved_queries (query_id, name, sql_text, description, created_at)
      VALUES (?, ?, ?, ?, ?)
    `).run(
      randomUUID(),
      `Query ${new Date().toISOString()}`,
      sql,
      `Query history entry (${rowCount} rows, db: ${databaseName ?? 'none'})`,
      new Date().toISOString(),
    );
  }

  // ── Events ────────────────────────────────────────────────────

  insertEvent(type: string, description: string, metadata?: Record<string, unknown>): void {
    this.db.prepare(`
      INSERT INTO events (type, description, metadata_json, created_at)
      VALUES (?, ?, ?, ?)
    `).run(
      type,
      description,
      metadata ? JSON.stringify(metadata) : null,
      new Date().toISOString(),
    );
  }

  // ── Connections ──────────────────────────────────────────

  insertConnection(id: string, name: string, type: string, configJson: string): void {
    this.db.prepare(`
      INSERT INTO connectors (connector_id, name, type, config_json, created_at)
      VALUES (?, ?, ?, ?, ?)
    `).run(id, name, type, configJson, new Date().toISOString());
  }

  listConnections(): Array<{ connectorId: string; name: string; type: string; configJson: string; createdAt: string; updatedAt?: string }> {
    const rows = this.db.prepare('SELECT * FROM connectors ORDER BY created_at DESC').all() as Record<string, unknown>[];
    return rows.map((row) => ({
      connectorId: row['connector_id'] as string,
      name: row['name'] as string,
      type: row['type'] as string,
      configJson: row['config_json'] as string,
      createdAt: row['created_at'] as string,
      updatedAt: (row['updated_at'] as string) ?? undefined,
    }));
  }

  getConnection(id: string): { connectorId: string; name: string; type: string; configJson: string; createdAt: string; updatedAt?: string } | undefined {
    const row = this.db.prepare('SELECT * FROM connectors WHERE connector_id = ?').get(id) as Record<string, unknown> | undefined;
    if (!row) return undefined;
    return {
      connectorId: row['connector_id'] as string,
      name: row['name'] as string,
      type: row['type'] as string,
      configJson: row['config_json'] as string,
      createdAt: row['created_at'] as string,
      updatedAt: (row['updated_at'] as string) ?? undefined,
    };
  }

  deleteConnection(id: string): void {
    this.db.prepare('DELETE FROM connectors WHERE connector_id = ?').run(id);
  }

  updateConnection(id: string, updates: { name?: string; configJson?: string }): void {
    const fields: string[] = [];
    const values: unknown[] = [];

    if (updates.name !== undefined) { fields.push('name = ?'); values.push(updates.name); }
    if (updates.configJson !== undefined) { fields.push('config_json = ?'); values.push(updates.configJson); }

    if (fields.length === 0) return;

    fields.push('updated_at = ?');
    values.push(new Date().toISOString());
    values.push(id);
    this.db.prepare(`UPDATE connectors SET ${fields.join(', ')} WHERE connector_id = ?`).run(...values);
  }

  // ── Chat Sessions ───────────────────────────────────────────

  listChatSessions(starred?: boolean): Array<{
    sessionId: string;
    title: string;
    starred: boolean;
    updatedAt: string;
    messageCount: number;
    lastMessagePreview?: string;
  }> {
    let sql = 'SELECT session_id, title, starred, updated_at, messages_json FROM chat_sessions';
    const params: unknown[] = [];
    if (starred !== undefined) {
      sql += ' WHERE starred = ?';
      params.push(starred ? 1 : 0);
    }
    sql += ' ORDER BY updated_at DESC';
    const rows = this.db.prepare(sql).all(...params) as Record<string, unknown>[];
    return rows.map((row) => {
      const messagesRaw = (row['messages_json'] as string) || '[]';
      let messages: Array<{ role?: string; content?: string }> = [];
      try { messages = JSON.parse(messagesRaw); } catch { /* ignore */ }
      const lastMsg = [...messages].reverse().find(m => m.role === 'assistant' || m.role === 'user');
      const preview = lastMsg?.content
        ? (typeof lastMsg.content === 'string' ? lastMsg.content.substring(0, 100) : undefined)
        : undefined;
      return {
        sessionId: row['session_id'] as string,
        title: (row['title'] as string) || 'Untitled',
        starred: (row['starred'] as number) === 1,
        updatedAt: (row['updated_at'] as string) || (row['created_at'] as string) || '',
        messageCount: messages.length,
        lastMessagePreview: preview,
      };
    });
  }

  getChatSession(sessionId: string): {
    sessionId: string;
    userId: string;
    title: string;
    starred: boolean;
    messagesJson: string;
    contextJson: string;
    compactedJson: string;
    compactionCount: number;
    createdAt: string;
    updatedAt: string;
  } | undefined {
    const row = this.db.prepare('SELECT * FROM chat_sessions WHERE session_id = ?').get(sessionId) as Record<string, unknown> | undefined;
    if (!row) return undefined;
    return {
      sessionId: row['session_id'] as string,
      userId: (row['user_id'] as string) || 'local',
      title: (row['title'] as string) || 'Untitled',
      starred: (row['starred'] as number) === 1,
      messagesJson: (row['messages_json'] as string) || '[]',
      contextJson: (row['context_json'] as string) || '{}',
      compactedJson: (row['compacted_json'] as string) || '[]',
      compactionCount: (row['compaction_count'] as number) || 0,
      createdAt: row['created_at'] as string,
      updatedAt: (row['updated_at'] as string) || (row['created_at'] as string),
    };
  }

  upsertChatSession(
    sessionId: string,
    title: string | undefined,
    messagesJson: string,
    options?: {
      userId?: string;
      starred?: boolean;
      contextJson?: string;
      compactionCount?: number;
      createdAt?: string;
      updatedAt?: string;
    },
  ): void {
    const now = options?.updatedAt || new Date().toISOString();
    const created = options?.createdAt || now;
    this.db.prepare(`
      INSERT INTO chat_sessions (session_id, title, messages_json, user_id, starred, context_json, compaction_count, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(session_id) DO UPDATE SET
        title = excluded.title,
        messages_json = excluded.messages_json,
        user_id = excluded.user_id,
        starred = excluded.starred,
        context_json = excluded.context_json,
        compaction_count = excluded.compaction_count,
        updated_at = excluded.updated_at
    `).run(
      sessionId,
      title ?? null,
      messagesJson,
      options?.userId ?? 'local',
      options?.starred ? 1 : 0,
      options?.contextJson ?? '{}',
      options?.compactionCount ?? 0,
      created,
      now,
    );
  }

  deleteChatSession(sessionId: string): void {
    this.db.prepare('DELETE FROM chat_sessions WHERE session_id = ?').run(sessionId);
  }

  starChatSession(sessionId: string, starred: boolean): boolean {
    const result = this.db.prepare(
      'UPDATE chat_sessions SET starred = ?, updated_at = ? WHERE session_id = ?',
    ).run(starred ? 1 : 0, new Date().toISOString(), sessionId);
    return (result as { changes: number }).changes > 0;
  }

  saveChatCompactedRange(sessionId: string, rangeJson: string): void {
    // Append range to compacted_json array and increment count
    const row = this.db.prepare('SELECT compacted_json, compaction_count FROM chat_sessions WHERE session_id = ?').get(sessionId) as Record<string, unknown> | undefined;
    if (!row) return;
    let ranges: unknown[] = [];
    try { ranges = JSON.parse((row['compacted_json'] as string) || '[]'); } catch { /* ignore */ }
    const newRange: unknown = JSON.parse(rangeJson);
    ranges.push(newRange);
    const count = ((row['compaction_count'] as number) || 0) + 1;
    this.db.prepare(
      'UPDATE chat_sessions SET compacted_json = ?, compaction_count = ?, updated_at = ? WHERE session_id = ?',
    ).run(JSON.stringify(ranges), count, new Date().toISOString(), sessionId);
  }

  getChatCompactedRanges(sessionId: string): string {
    const row = this.db.prepare('SELECT compacted_json FROM chat_sessions WHERE session_id = ?').get(sessionId) as Record<string, unknown> | undefined;
    if (!row) return '[]';
    return (row['compacted_json'] as string) || '[]';
  }

  close(): void {
    this.db.close();
  }
}
