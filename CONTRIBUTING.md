# Contributing to Nile API Local

We welcome contributions! This document provides guidelines for contributing to the project.

## Development Setup

1. **Prerequisites**: Node.js 22+, Podman (for Spark), Ollama (optional, for AI features)
2. Clone the repo and install dependencies:
   ```bash
   npm install
   ```
3. Start in dev mode:
   ```bash
   API_LOCAL_PORT=3001 npm run dev
   ```

## Project Structure

```
src/
  handlers/     : HTTP request handlers by domain
  services/     : Core services (SQLite, query worker pool)
  engines/      : Compute engines (Spark via Podman)
  config/       : Configuration loading
  types/        : Type definitions, route metadata, AI tool schemas
  lib/          : Vendored shared libraries
  types/        : Local type definitions
  spark/        : Spark container files
```

## Code Style

- TypeScript strict mode (`strict: true`, `noImplicitAny: true`)
- ESM modules throughout
- Handler factory pattern: each handler exports a `create*Handler()` function
- Structured logging with `[prefix]` tags (e.g., `[api-local]`, `[spark-engine]`)

## Making Changes

1. Create a feature branch
2. Make your changes
3. Run `npm run build` to verify TypeScript compilation
4. Test manually against the `/health` endpoint and relevant handlers
5. Submit a pull request with a clear description

## Type Definitions

Files in `src/types/` contain the core type definitions, route metadata, and AI tool schemas. Do not edit them directly. If you need type changes, open an issue to discuss.

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
