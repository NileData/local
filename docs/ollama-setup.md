# Ollama Setup

Ollama provides local LLM inference for the AI chat features. It's optional: all other features (queries, imports, catalog) work without it.

## Install Ollama

Download from [ollama.com](https://ollama.com) or install via package manager:

```bash
# macOS
brew install ollama

# Linux
curl -fsSL https://ollama.com/install.sh | sh
```

## Pull a Model

The default model is `qwen3-coder`. Pull it before first use:

```bash
ollama pull qwen3-coder
```

Other recommended models for data analysis:

| Model | Size | Best For |
|-------|------|----------|
| `qwen3-coder` | ~5 GB | General data queries and code generation |
| `llama3.1:8b` | ~4.7 GB | General purpose, good balance |
| `mistral` | ~4.1 GB | Fast responses, lighter weight |
| `codellama:13b` | ~7.4 GB | SQL and code generation |

## Start Ollama

Ollama runs as a background service:

```bash
ollama serve
```

By default it listens on port `11434`. If you use a different port, update `~/.nile/config.toml`:

```toml
[ai]
local_port = 11434
```

Or set the port at runtime via the API:

```bash
curl -X POST http://localhost:3001/ollama/port \
  -H "Content-Type: application/json" \
  -d '{"port": 11434}'
```

## Verify Connection

```bash
# Check Ollama status via the API
curl http://localhost:3001/ollama/status

# List available models
curl http://localhost:3001/ollama/models
```

## Using AI Chat

Once Ollama is running with a model pulled:

```bash
# Start a chat
curl -X POST http://localhost:3001/chat/invoke \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {"role": "user", "content": "Show me all tables in the my_data database"}
    ],
    "sessionId": "test-session"
  }'
```

The AI assistant has access to all catalog and query tools: it can list databases, inspect schemas, execute queries, and generate visualizations.

## Model Warmup

To pre-load a model into VRAM for faster first response:

```bash
curl -X POST http://localhost:3001/ollama/warmup \
  -H "Content-Type: application/json" \
  -d '{"model": "qwen3-coder"}'
```

## Changing Models

Update the default model in `~/.nile/config.toml`:

```toml
[ai]
model = "llama3.1:8b"
```

Or pass the model per-request in the chat invoke body.

## Custom Skills

You can create custom AI skills as markdown files in `~/.nile/skills/`:

```bash
# List skills
curl http://localhost:3001/ai/skills

# Create a skill
curl -X POST http://localhost:3001/ai/skills \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-analysis",
    "content": "# My Analysis Skill\n\nWhen asked to analyze sales data..."
  }'
```

## Troubleshooting

**"Ollama not available"**: Ensure `ollama serve` is running and the port matches your config.

**Slow first response**: The model needs to load into VRAM on first use. Use the warmup endpoint to pre-load.

**Out of memory**: Try a smaller model (e.g., `mistral` instead of `codellama:13b`), or increase system swap.
