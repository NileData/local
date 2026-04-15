# Nile API Local

A local data API server that powers the [Nile desktop data IDE](https://getnile.ai/downloads). Query your data with SQL or PySpark, import from any source, and chat with an AI data assistant: all running entirely on your machine.

## Download

Get the full Nile desktop app at **[getnile.ai/downloads](https://getnile.ai/downloads)**

[![Nile Local Demo](https://img.youtube.com/vi/C6qSFLylryk/maxresdefault.jpg)](https://www.youtube.com/watch?v=C6qSFLylryk)

## Features

- **Query Engine**: Execute SQL and PySpark queries via local Apache Spark (Podman containers)
- **Data Catalog**: SQLite-backed catalog for databases, tables, and schemas
- **Data Import**: Import from CSV, Parquet, JSON, S3, PostgreSQL, Snowflake, AWS Glue, or paste
- **AI Chat**: Chat with a local LLM via Ollama for data analysis, query generation, and insights
- **Connection Management**: Store and manage credentials for external data sources
- **Table Metadata**: Dependencies, snapshots, schema introspection via Iceberg

## Prerequisites

- **Node.js 22+** (required for built-in SQLite support)
- **Podman** (for running Apache Spark containers)
- **Ollama** (optional, for AI chat features)

## Quick Start

```bash
# Install dependencies
npm install

# Build
npm run build

# Start the server (specify a port)
API_LOCAL_PORT=3001 node dist/bundle/server.js

# Check health
curl http://localhost:3001/health
```

## Configuration

The server reads configuration from `~/.nile/config.toml`. See [docs/configuration.md](docs/configuration.md) for all options.

## Architecture

- **Express HTTP server** with auto-wired route definitions
- **SQLite catalog** (`~/.nile/catalog.db`) for metadata storage
- **Apache Spark** via Podman for distributed query execution
- **Ollama** integration for local LLM chat
- **Query Worker Pool** for concurrent query execution

## Building

```bash
# TypeScript compilation
npm run build

# Single-file bundle (for deployment)
npm run build:bundle
```

## License

[Apache License 2.0](LICENSE)

## Third-Party Notices

This project optionally integrates with:
- **Ollama** for local LLM inference
- **Apache Spark** for query execution (via Podman containers)
- **AWS SDK** for S3 and Glue data imports (credentials from your local environment)
