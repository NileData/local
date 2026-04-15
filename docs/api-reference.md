# API Reference

Complete reference for all HTTP endpoints provided by Nile API Local. Routes marked **local-only** are specific to the local server.

Base URL: `http://localhost:{API_LOCAL_PORT}`

---

## System

| Method | Path | Description | Handler |
|--------|------|-------------|---------|
| `GET` | `/health` | Health check (Spark status, SQLite, data dir) | [health.ts](../src/handlers/health.ts) |
| `GET` | `/config` | Server configuration (team name, compute settings) | [config.ts](../src/handlers/config.ts) |

---

## Query Execution

| Method | Path | Description | Handler |
|--------|------|-------------|---------|
| `POST` | `/query` | Execute a SQL or PySpark query | [query/execute.ts](../src/handlers/query/execute.ts) |
| `GET` | `/query/{queryExecutionId}/status` | Poll query execution status | [query/get-status.ts](../src/handlers/query/get-status.ts) |
| `GET` | `/query/{queryExecutionId}/results` | Fetch query results (paginated) | [query/get-results.ts](../src/handlers/query/get-results.ts) |
| `POST` | `/query/{queryExecutionId}/stop` | Cancel a running query | [query/stop.ts](../src/handlers/query/stop.ts) |
| `POST` | `/query/save-as-table` | Create an Iceberg table from query results | [query/save-as-table.ts](../src/handlers/query/save-as-table.ts) |
| `POST` | `/query/visuals` | Generate visualization plan via LLM (requires Ollama) | [query/generate-visuals.ts](../src/handlers/query/generate-visuals.ts) |
| `POST` | `/query/visuals/full-data` | Execute aggregation SQL for chart rendering | [query/execute-visual-full-data.ts](../src/handlers/query/execute-visual-full-data.ts) |

### Query Workflow

1. `POST /query` with `{ database, code: { language: "sql", content: "SELECT ..." } }`: returns `{ jobId }`
2. `GET /query/{jobId}/status`: poll until `status` is `COMPLETED` or `FAILED`
3. `GET /query/{jobId}/results`: fetch result rows and columns

---

## Databases

| Method | Path | Description | Handler |
|--------|------|-------------|---------|
| `GET` | `/databases` | List all databases | [database/list.ts](../src/handlers/database/list.ts) |
| `GET` | `/databases/{database}` | Get database details | [database/get.ts](../src/handlers/database/get.ts) |
| `POST` | `/databases` | Create a new database | [database/create.ts](../src/handlers/database/create.ts) |
| `DELETE` | `/databases/{database}` | Delete a database | [database/delete.ts](../src/handlers/database/delete.ts) |

---

## Tables

| Method | Path | Description | Handler |
|--------|------|-------------|---------|
| `GET` | `/tables` | List tables (optionally filtered by database) | [table/list.ts](../src/handlers/table/list.ts) |
| `GET` | `/tables/{database}/{table}` | Get table metadata with live row count | [table/get.ts](../src/handlers/table/get.ts) |
| `POST` | `/tables` | Create a table (empty, from file, or CTAS) | [table/create.ts](../src/handlers/table/create.ts) |
| `DELETE` | `/tables/{database}/{table}` | Delete a table | [table/delete.ts](../src/handlers/table/delete.ts) |
| `GET` | `/tables/{database}/{table}/schema` | Get live table schema from Spark | [table/get-schema.ts](../src/handlers/table/get-schema.ts) |
| `GET` | `/tables/{database}/{table}/dependencies` | Get upstream table dependencies | [table/dependencies.ts](../src/handlers/table/dependencies.ts) |
| `GET` | `/tables/{database}/{table}/dependents` | Get downstream dependents | [table/dependents.ts](../src/handlers/table/dependents.ts) |
| `GET` | `/tables/{database}/{table}/branches` | List Iceberg branches | [table/list-branches.ts](../src/handlers/table/list-branches.ts) |
| `GET` | `/tables/{database}/{table}/snapshots` | List Iceberg snapshots (time travel) | [table/list-snapshots.ts](../src/handlers/table/list-snapshots.ts) |

---

## Search

| Method | Path | Description | Handler |
|--------|------|-------------|---------|
| `GET` | `/search?searchQuery={q}` | Fuzzy search across tables, databases, columns | [search.ts](../src/handlers/search.ts) |

---

## Data Import

| Method | Path | Description | Handler |
|--------|------|-------------|---------|
| `POST` | `/import/paste` | Import from pasted CSV/JSON data | [import/paste.ts](../src/handlers/import/paste.ts) |
| `POST` | `/import/local-file` | Import from a local file path **local-only** | [import/local-file.ts](../src/handlers/import/local-file.ts) |
| `POST` | `/import/detect` | Detect schema from file or S3 path | [import/detect.ts](../src/handlers/import/detect.ts) |
| `POST` | `/import/s3` | Import from Amazon S3 | [import/s3.ts](../src/handlers/import/s3.ts) |
| `POST` | `/import/postgres` | Import from PostgreSQL via JDBC | [import/postgres.ts](../src/handlers/import/postgres.ts) |
| `POST` | `/import/snowflake` | Import from Snowflake | [import/snowflake.ts](../src/handlers/import/snowflake.ts) |
| `POST` | `/import/glue` | Import from AWS Glue catalog | [import/glue.ts](../src/handlers/import/glue.ts) |

---

## Connections

Manage stored credentials for external data sources (PostgreSQL, Snowflake, MySQL, etc.).

| Method | Path | Description | Handler |
|--------|------|-------------|---------|
| `POST` | `/connections` | Create a connection profile | [connection-api.ts](../src/handlers/connection-api.ts) |
| `GET` | `/connections` | List all connections | [connection-api.ts](../src/handlers/connection-api.ts) |
| `GET` | `/connections/{connectionId}` | Get connection details | [connection-api.ts](../src/handlers/connection-api.ts) |
| `DELETE` | `/connections/{connectionId}` | Delete a connection | [connection-api.ts](../src/handlers/connection-api.ts) |
| `POST` | `/connections/test` | Test a new connection (without saving) | [connection-api.ts](../src/handlers/connection-api.ts) |
| `POST` | `/connections/{connectionId}/test` | Test a saved connection | [connection-api.ts](../src/handlers/connection-api.ts) |
| `POST` | `/connections/{connectionId}/tables` | List tables in external database | [connection-api.ts](../src/handlers/connection-api.ts) |
| `POST` | `/connections/{connectionId}/schema` | Detect schema of an external table | [connection-api.ts](../src/handlers/connection-api.ts) |
| `POST` | `/connections/{connectionId}/query` | Run read-only SQL on external database | [connection-api.ts](../src/handlers/connection-api.ts) |
| `POST` | `/connections/{connectionId}/distribution` | Analyze column value distribution | [connection-api.ts](../src/handlers/connection-api.ts) |

---

## AI Chat

### Sessions

| Method | Path | Description | Handler |
|--------|------|-------------|---------|
| `GET` | `/ai/sessions` | List chat sessions | [ai-chat.ts](../src/handlers/ai-chat.ts) |
| `GET` | `/ai/sessions/{sessionId}` | Get session with messages | [ai-chat.ts](../src/handlers/ai-chat.ts) |
| `PUT` | `/ai/sessions/{sessionId}` | Update session title | [ai-chat.ts](../src/handlers/ai-chat.ts) |
| `DELETE` | `/ai/sessions/{sessionId}` | Delete a session | [ai-chat.ts](../src/handlers/ai-chat.ts) |
| `PUT` | `/ai/sessions/{sessionId}/star` | Star/unstar a session | [ai-chat.ts](../src/handlers/ai-chat.ts) |
| `POST` | `/ai/sessions/{sessionId}/title` | Auto-generate session title via LLM | [ai-chat.ts](../src/handlers/ai-chat.ts) |
| `GET` | `/ai/sessions/{sessionId}/compacted` | Get compacted message ranges | [ai-chat.ts](../src/handlers/ai-chat.ts) |
| `POST` | `/ai/sessions/{sessionId}/compacted` | Save compacted message range | [ai-chat.ts](../src/handlers/ai-chat.ts) |

### Chat Invocation

| Method | Path | Description | Handler |
|--------|------|-------------|---------|
| `POST` | `/chat/invoke` | Send message to LLM (request/response) | [chat/invoke.ts](../src/handlers/chat/invoke.ts) |
| `POST` | `/chat/invoke-streaming` | Send message to LLM (SSE streaming) **local-only** | [chat/invoke-streaming.ts](../src/handlers/chat/invoke-streaming.ts) |

Both invoke endpoints proxy to Ollama's Anthropic-compatible `/v1/messages` endpoint. They include all AI tool schemas, allowing the LLM to call catalog, query, and import tools.

---

## AI Skills, Commands, and Memory

### Skills

Custom AI skills stored as markdown files in `~/.nile/skills/`.

| Method | Path | Description | Handler |
|--------|------|-------------|---------|
| `GET` | `/ai/skills` | List all skills | [ai/skills.ts](../src/handlers/ai/skills.ts) |
| `GET` | `/ai/skills/{name}` | Get a skill | [ai/skills.ts](../src/handlers/ai/skills.ts) |
| `POST` | `/ai/skills` | Create a skill | [ai/skills.ts](../src/handlers/ai/skills.ts) |
| `PUT` | `/ai/skills/{name}` | Update a skill | [ai/skills.ts](../src/handlers/ai/skills.ts) |
| `DELETE` | `/ai/skills/{name}` | Delete a skill | [ai/skills.ts](../src/handlers/ai/skills.ts) |

### Commands

Custom AI commands stored in `~/.nile/commands/`.

| Method | Path | Description | Handler |
|--------|------|-------------|---------|
| `GET` | `/ai/commands` | List all commands | [ai/commands.ts](../src/handlers/ai/commands.ts) |
| `GET` | `/ai/commands/{name}` | Get a command | [ai/commands.ts](../src/handlers/ai/commands.ts) |
| `POST` | `/ai/commands` | Create a command | [ai/commands.ts](../src/handlers/ai/commands.ts) |
| `PUT` | `/ai/commands/{name}` | Update a command | [ai/commands.ts](../src/handlers/ai/commands.ts) |
| `DELETE` | `/ai/commands/{name}` | Delete a command | [ai/commands.ts](../src/handlers/ai/commands.ts) |

### Memory

Persistent AI memory stored in `~/.nile/memory/MEMORY.md`.

| Method | Path | Description | Handler |
|--------|------|-------------|---------|
| `GET` | `/ai/memory` | Get persistent memory content | [ai/memory.ts](../src/handlers/ai/memory.ts) |
| `PUT` | `/ai/memory` | Update persistent memory | [ai/memory.ts](../src/handlers/ai/memory.ts) |

---

## Ollama Management

Local-only endpoints for managing the Ollama LLM runtime.

| Method | Path | Description | Handler |
|--------|------|-------------|---------|
| `POST` | `/ollama/port` | Set Ollama port at runtime | [server.ts](../src/server.ts) (inline) |
| `POST` | `/ollama/stop-model` | Unload model from VRAM | [server.ts](../src/server.ts) (inline) |
| `POST` | `/ollama/warmup` | Pre-load model into VRAM | [server.ts](../src/server.ts) (inline) |
| `GET` | `/ollama/models` | List installed Ollama models | [server.ts](../src/server.ts) (inline) |
| `GET` | `/ollama/status` | Check Ollama connection and loaded models | [server.ts](../src/server.ts) (inline) |

---

## Export

| Method | Path | Description | Handler |
|--------|------|-------------|---------|
| `POST` | `/export/chart-image` | Render chart as image **experimental** | [export/chart-image.ts](../src/handlers/export/chart-image.ts) |

---

## Handler Source Map

Quick reference for finding handler source code by directory:

```
src/handlers/
  ai-chat.ts          : Chat session CRUD (list, get, delete, star, compact)
  ai/
    commands.ts        : Custom AI commands CRUD
    memory.ts          : Persistent AI memory
    skills.ts          : Custom AI skills CRUD
  chat/
    invoke.ts          : LLM chat (request/response via Ollama)
    invoke-streaming.ts: LLM chat (SSE streaming via Ollama)
  config.ts            : Server configuration endpoint
  connection-api.ts    : External database connection management (10 endpoints)
  database/
    create.ts          : Create database
    delete.ts          : Delete database
    get.ts             : Get database details
    list.ts            : List databases
  export/
    chart-image.ts     : Chart image rendering (experimental)
  health.ts            : Health check
  import/
    detect.ts          : Schema detection from files/S3
    glue.ts            : Import from AWS Glue
    local-file.ts      : Import from local filesystem
    paste.ts           : Import from pasted CSV/JSON
    postgres.ts        : Import from PostgreSQL
    s3-credentials.ts  : AWS credential resolution for S3
    s3.ts              : Import from Amazon S3
    snowflake.ts       : Import from Snowflake
  query/
    execute.ts         : Execute SQL/PySpark query
    execute-visual-full-data.ts: Aggregation SQL for charts
    generate-visuals.ts: LLM-powered visualization plans
    get-results.ts     : Fetch query results
    get-status.ts      : Poll query status
    save-as-table.ts   : Create table from query results
    stop.ts            : Cancel running query
  search.ts            : Catalog fuzzy search
  table/
    create.ts          : Create table (empty, file, CTAS, PySpark SAT)
    delete.ts          : Delete table
    dependencies.ts    : Get upstream dependencies
    dependents.ts      : Get downstream dependents
    get-schema.ts      : Live schema from Spark
    get.ts             : Table metadata with row count
    list-branches.ts   : Iceberg branches
    list-snapshots.ts  : Iceberg snapshots
    list.ts            : List tables
```
