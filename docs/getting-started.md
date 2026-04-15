# Getting Started

## Prerequisites

- **Node.js 22+**: Required for the built-in `node:sqlite` module used for the local catalog
- **Podman**: Container runtime for Apache Spark query execution ([podman.io](https://podman.io))
- **Ollama** (optional): Local LLM for AI chat features ([ollama.com](https://ollama.com))

### Verify Prerequisites

```bash
node --version    # Must be v22.0.0 or higher
podman --version  # Any recent version
ollama --version  # Optional
```

## Installation

```bash
git clone https://github.com/NileData/local.git
cd local
npm install
```

## Building

```bash
# TypeScript compilation (outputs to dist/)
npm run build

# Single-file bundle (outputs to dist/bundle/server.js)
npm run build:bundle
```

## Starting the Server

```bash
# Set the port and start
API_LOCAL_PORT=3001 node dist/server.js

# Or use the bundled version
API_LOCAL_PORT=3001 node dist/bundle/server.js
```

The server will:
1. Load configuration from `~/.nile/config.toml` (created with defaults if missing)
2. Initialize the SQLite catalog at `~/.nile/catalog.db`
3. Start Spark via Podman in the background (may take 30-60 seconds on first run)
4. Listen on the specified port

## Verify It Works

```bash
# Health check
curl http://localhost:3001/health

# List databases
curl http://localhost:3001/databases

# Create a database
curl -X POST http://localhost:3001/databases \
  -H "Content-Type: application/json" \
  -d '{"name": "my_data"}'
```

## First Query

Once Spark is ready (check `/health` for `sparkStatus: "ready"`):

```bash
# Execute a SQL query
curl -X POST http://localhost:3001/query/execute \
  -H "Content-Type: application/json" \
  -d '{
    "database": "my_data",
    "code": { "language": "sql", "content": "SELECT 1 AS hello, current_date() AS today" }
  }'
```

The response includes a `jobId`. Poll for results:

```bash
# Check status
curl http://localhost:3001/query/{jobId}/status

# Get results (once status is COMPLETED)
curl http://localhost:3001/query/{jobId}/results
```

## Importing Data

### From a local CSV file

```bash
curl -X POST http://localhost:3001/import/local-file \
  -H "Content-Type: application/json" \
  -d '{
    "database": "my_data",
    "tableName": "sales",
    "filePath": "/path/to/sales.csv",
    "fileFormat": "csv"
  }'
```

### From pasted data

```bash
curl -X POST http://localhost:3001/import/paste \
  -H "Content-Type: application/json" \
  -d '{
    "database": "my_data",
    "tableName": "quick_data",
    "content": "name,age,city\nAlice,30,NYC\nBob,25,SF",
    "format": "csv"
  }'
```

## Development Mode

For development with hot reload:

```bash
API_LOCAL_PORT=3001 npm run dev
```

## Next Steps

- [Configuration Reference](configuration.md): Customize Spark, Ollama, and storage settings
- [Architecture Overview](architecture.md): Understand the system design
- [Ollama Setup](ollama-setup.md): Enable AI chat features
