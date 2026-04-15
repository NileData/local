# Configuration

The server reads configuration from `~/.nile/config.toml`. If the file doesn't exist, defaults are used and the file is created on first run.

## Full Configuration Reference

```toml
[ai]
mode = "web-agent"           # AI mode: "web-agent", "claude-api", "claude-bedrock"
provider = "ollama"          # LLM provider
model = "qwen3-coder"       # Default Ollama model
local_port = 11434           # Ollama HTTP port
api_key = ""                 # API key (for cloud providers)

[local]
data_dir = "~/.nile"         # Base data directory
max_result_rows = 10000      # Max rows returned per query
query_timeout_warn_ms = 300000  # Query timeout warning (5 min)
team_name = "local"          # Team/workspace name

[compute]
default_engine = "spark"     # Compute engine
spark_enabled = true         # Enable/disable Spark
spark_port = 3002            # Spark Connect server port
spark_driver_memory_gb = 4   # Driver memory (auto-scaled if absent)
max_concurrent_queries = 2   # Worker pool size
spark_prewarm = false        # Pre-warm Spark on startup
```

## Section Details

### `[ai]` -- AI Assistant Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `mode` | `"web-agent"` | AI interaction mode |
| `provider` | `"ollama"` | LLM provider name |
| `model` | `"qwen3-coder"` | Model to use for chat |
| `local_port` | `11434` | Ollama API port |
| `api_key` | `""` | API key for cloud AI providers |

### `[local]` -- Local Storage

| Key | Default | Description |
|-----|---------|-------------|
| `data_dir` | `"~/.nile"` | Root directory for all data (catalog, data lake, results) |
| `max_result_rows` | `10000` | Maximum rows returned in query results |
| `query_timeout_warn_ms` | `300000` | Warn after this many ms (5 min) |
| `team_name` | `"local"` | Workspace/team identifier |

### `[compute]` -- Query Engine

| Key | Default | Description |
|-----|---------|-------------|
| `default_engine` | `"spark"` | Compute engine to use |
| `spark_enabled` | `true` | Whether to initialize Spark on startup |
| `spark_port` | `3002` | Port for Spark Connect HTTP bridge |
| `spark_driver_memory_gb` | Auto | Driver memory in GB. If absent, auto-scales to 25% of system RAM (max 16 GB) |
| `max_concurrent_queries` | `2` | Number of concurrent Spark workers |
| `spark_prewarm` | `false` | Start Spark container immediately on server boot |

## Environment Variables

These override config file settings:

| Variable | Description |
|----------|-------------|
| `API_LOCAL_PORT` | HTTP server listen port (required) |
| `PODMAN_BIN_DIR` | Path to Podman binaries (macOS bundled) |
| `PODMAN_INSTALLER_PATH` | Path to Podman installer (Windows) |
| `NILE_OLLAMA_PORT` | Override Ollama port at startup |

## Auto-Scaling Memory

If `spark_driver_memory_gb` is not set in config, the server automatically calculates:

```
driverMemory = max(1, min(16, floor(totalSystemRAM * 0.25)))
```

This gives Spark a reasonable share of RAM without starving the OS or other applications.
