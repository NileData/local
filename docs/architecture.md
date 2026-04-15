# Architecture

## Overview

Nile API Local is an Express HTTP server that provides a complete data platform API running entirely on your machine. It manages a local data catalog, executes queries via Apache Spark, and integrates with Ollama for AI-assisted data analysis.

```
                    HTTP Clients (curl, UI, scripts)
                              |
                    +---------v---------+
                    |   Express Server  |
                    |   (server.ts)     |
                    +---+-------+---+---+
                        |       |   |
              +---------+   +---+   +--------+
              |             |                |
     +--------v------+ +---v--------+ +-----v------+
     | SQLite Catalog | | Spark      | | Ollama     |
     | (catalog.db)   | | (Podman)   | | (HTTP)     |
     +----------------+ +------------+ +------------+
```

## Components

### Express Server (`src/server.ts`)

The entry point. Loads configuration, initializes services, and wires up all routes. Route definitions live in `src/generated/routes.ts` and handler implementations are mapped in the `implementedRoutes` object.

### SQLite Catalog (`src/services/sqlite-service.ts`)

Stores all metadata in `~/.nile/catalog.db` using Node.js 22's built-in `node:sqlite` module in WAL mode for concurrent access. Tables:

| Table | Purpose |
|-------|---------|
| `databases` | Database metadata |
| `tables` | Table catalog (schema, row count, type) |
| `jobs` | Query/import execution history |
| `saved_queries` | Query history |
| `connectors` | Stored credentials |
| `chat_sessions` | AI chat sessions and messages |
| `events` | System event log |
| `config` | Configuration key-value store |

### Apache Spark Engine (`src/engines/spark-engine.ts`)

Manages Spark via Podman containers. The `nile-spark-connect` Docker image includes:
- Spark 3.5.4 with Spark Connect
- Apache Iceberg (branch-based versioning, ACID transactions)
- JDBC connectors (PostgreSQL, Snowflake)
- Cloud storage connectors (S3, GCS, Azure Blob)

Key features:
- **Auto-scaling memory**: Driver memory set to 25% of system RAM (max 16 GB)
- **Container reuse**: Spark containers are reused across queries to avoid startup overhead
- **Health polling**: Background health checks with configurable timeouts

### Query Worker Pool (`src/services/query-worker-pool.ts`)

Manages concurrent query execution across multiple Spark instances:
- Fair scheduling queue
- Configurable concurrency (`max_concurrent_queries` in config)
- Worker recycling to prevent resource leaks
- Runtime state tracking for progress reporting

### Ollama Integration (`src/handlers/chat/`)

AI chat via Ollama's Anthropic-compatible `/v1/messages` endpoint:
- Tool-use loop with AI tool schemas
- System prompt from shared configuration
- Streaming (SSE) and request/response modes
- Custom skills and commands (`~/.nile/skills/`, `~/.nile/commands/`)

## Data Storage

```
~/.nile/
  config.toml       : Server configuration
  catalog.db        : SQLite metadata catalog
  data-lake/        : Spark warehouse (Iceberg tables as Parquet)
  operations/
    results/         : Query result sets (Parquet files)
  skills/           : Custom AI skills (markdown files)
  commands/         : Custom AI commands (markdown files)
  memory/           : AI persistent memory
```

## Handler Architecture

Handlers follow a factory pattern:

```typescript
export function createListTablesHandler(
  config: LocalConfig,
  sparkEngine: SparkEngine,
  sqliteService: SQLiteService
): RequestHandler {
  return async (req, res) => {
    // Handler logic
  };
}
```

Dependencies (config, engine, SQLite) are injected at server startup. This makes handlers testable and decoupled from infrastructure.

## Types and Route Definitions

Core type definitions and route metadata live in `src/types/`:
- `types.ts`: All API request/response types and enums
- `routes.ts`: HTTP route definitions (method, path, handler ID)
- `ai-tool-schemas.ts`: AI tool definitions for the LLM

## Import Pipeline

All imports follow the same pattern:
1. Validate input (file path, S3 URI, connection ID, etc.)
2. Create a job record in SQLite (`status: RUNNING`)
3. Stage data if needed (copy to temp location)
4. Use Spark to read the source and write as an Iceberg table
5. Update the catalog with schema and row count
6. Update job status (`COMPLETED` or `FAILED`)
