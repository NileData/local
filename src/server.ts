import express from 'express';
import cors from 'cors';
import { join } from 'path';
import { mkdirSync } from 'fs';
import { totalmem, homedir } from 'os';
import type { RequestHandler } from 'express';
import { loadConfig } from './config/environment.js';
import { SparkEngine } from './engines/spark-engine.js';
import { EngineRegistry } from './engines/engine-registry.js';
import { SQLiteService } from './services/sqlite-service.js';
import { ROUTES } from './types/routes.js';
import { createQueryExecuteHandler } from './handlers/query/execute.js';
import { createQueryStatusHandler } from './handlers/query/get-status.js';
import { createQueryResultsHandler } from './handlers/query/get-results.js';
import { createQueryStopHandler } from './handlers/query/stop.js';
import { createConfigHandler } from './handlers/config.js';
import { createHealthHandler } from './handlers/health.js';
import { createDatabaseListHandler } from './handlers/database/list.js';
import { createDatabaseGetHandler } from './handlers/database/get.js';
import { createTableListHandler } from './handlers/table/list.js';
import { createTableGetHandler } from './handlers/table/get.js';
import { createTableSchemaHandler } from './handlers/table/get-schema.js';
import { createSearchHandler } from './handlers/search.js';
import { createDatabaseCreateHandler } from './handlers/database/create.js';
import { createDatabaseDeleteHandler } from './handlers/database/delete.js';
import { createTableCreateHandler } from './handlers/table/create.js';
import { createTableDeleteHandler } from './handlers/table/delete.js';
import { createImportDetectHandler } from './handlers/import/detect.js';
import { createImportPasteHandler } from './handlers/import/paste.js';
import { createLocalFileImportHandler } from './handlers/import/local-file.js';
import { createImportS3Handler } from './handlers/import/s3.js';
import { createImportPostgresHandler } from './handlers/import/postgres.js';
import { createImportSnowflakeHandler } from './handlers/import/snowflake.js';
import { createImportGlueHandler } from './handlers/import/glue.js';
import { createConnectionHandlers } from './handlers/connection-api.js';
import { createSaveAsTableHandler } from './handlers/query/save-as-table.js';
import { createTableDependenciesHandler } from './handlers/table/dependencies.js';
import { createTableDependentsHandler } from './handlers/table/dependents.js';
import { createListBranchesHandler } from './handlers/table/list-branches.js';
import { createListSnapshotsHandler } from './handlers/table/list-snapshots.js';
import { createAiChatHandlers } from './handlers/ai-chat.js';
import { createChartImageHandler } from './handlers/export/chart-image.js';
import { createChatInvokeHandler } from './handlers/chat/invoke.js';
import { createChatInvokeStreamingHandler } from './handlers/chat/invoke-streaming.js';
import { createSkillHandlers } from './handlers/ai/skills.js';
import { createCommandHandlers } from './handlers/ai/commands.js';
import { createMemoryHandlers } from './handlers/ai/memory.js';
import { QueryWorkerPool } from './services/query-worker-pool.js';
import { createGenerateVisualsHandler } from './handlers/query/generate-visuals.js';
import { createExecuteVisualFullDataHandler } from './handlers/query/execute-visual-full-data.js';

const PORT = (() => {
  const raw = process.env.API_LOCAL_PORT;
  if (!raw) {
    throw new Error(
      '[api-local] FATAL: API_LOCAL_PORT env var is not set. ' +
      'The Rust parent process must allocate a port and pass it via API_LOCAL_PORT.',
    );
  }
  const port = parseInt(raw, 10);
  if (isNaN(port) || port < 1 || port > 65535) {
    throw new Error(`[api-local] FATAL: Invalid API_LOCAL_PORT value: "${raw}"`);
  }
  return port;
})();

/** Auto-scale Spark driver memory: max(1, min(16, floor(totalRAM * 0.25))) GB */
function autoScaleDriverMemory(): number {
  const totalGb = totalmem() / (1024 ** 3);
  return Math.max(1, Math.min(16, Math.floor(totalGb * 0.25)));
}

async function main(): Promise<void> {
  console.log(`[api-local] Starting on port ${PORT}...`);

  // 1. Load config
  const config = loadConfig();
  console.log(`[api-local] Config: sparkEnabled=${config.compute.sparkEnabled}, sparkPort=${config.compute.sparkPort}, PODMAN_BIN_DIR=${process.env['PODMAN_BIN_DIR'] ?? '(not set)'}, PODMAN_INSTALLER_PATH=${process.env['PODMAN_INSTALLER_PATH'] ?? '(not set)'}`);

  // 2. Initialize SQLite
  const dataDir = config.local.dataDir;
  mkdirSync(dataDir, { recursive: true });
  mkdirSync(join(dataDir, 'operations', 'results'), { recursive: true });
  mkdirSync(join(dataDir, 'data-lake'), { recursive: true });
  const catalogPath = join(dataDir, 'catalog.db');
  const sqliteService = new SQLiteService(catalogPath);
  console.log(`[api-local] SQLite ready (${catalogPath})`);

  // 3. Create engine registry
  const driverMemoryGb = config.compute.sparkDriverMemoryGb ?? autoScaleDriverMemory();
  const sparkEngine = new SparkEngine(config.compute.sparkPort, dataDir, driverMemoryGb);
  const engineRegistry = new EngineRegistry(sparkEngine);
  const queryWorkerPool = new QueryWorkerPool(config, sqliteService, sparkEngine);

  // 4. Seed database immediately (catalog-only, no Spark needed) and purge stale entries
  seedDefaultDatabaseFallback(sqliteService, config.local.teamName);
  purgeUnauthorizedDatabasesCatalogOnly(sqliteService, config.local.teamName);

  // 5. Create Express app
  const app = express();
  app.use(cors());
  app.use(express.json());

  // 6. Create handler instances
  const queryExecuteHandler = createQueryExecuteHandler(config, queryWorkerPool, sqliteService);
  const queryStatusHandler = createQueryStatusHandler(queryWorkerPool, sqliteService);
  const queryResultsHandler = createQueryResultsHandler(queryWorkerPool, sqliteService);
  const queryStopHandler = createQueryStopHandler(queryWorkerPool, sqliteService);
  const configHandler = createConfigHandler(config);
  const databaseListHandler = createDatabaseListHandler(sqliteService);
  const databaseGetHandler = createDatabaseGetHandler(sqliteService);
  const tableListHandler = createTableListHandler(sqliteService);
  const tableGetHandler = createTableGetHandler(sparkEngine, sqliteService);
  const tableSchemaHandler = createTableSchemaHandler(sparkEngine);
  const searchHandler = createSearchHandler(sqliteService);
  const databaseCreateHandler = createDatabaseCreateHandler(config, sparkEngine, sqliteService);
  const databaseDeleteHandler = createDatabaseDeleteHandler(config, sparkEngine, sqliteService);
  const tableCreateHandler = createTableCreateHandler(config, sparkEngine, sqliteService);
  const tableDeleteHandler = createTableDeleteHandler(sparkEngine, sqliteService);
  const importDetectHandler = createImportDetectHandler(sparkEngine);
  const importPasteHandler = createImportPasteHandler(config, sparkEngine, sqliteService);
  const localFileImportHandler = createLocalFileImportHandler(sparkEngine, sqliteService);
  const importS3Handler = createImportS3Handler(sparkEngine, sqliteService);
  const importPostgresHandler = createImportPostgresHandler(sparkEngine, sqliteService);
  const importSnowflakeHandler = createImportSnowflakeHandler(sparkEngine, sqliteService);
  const importGlueHandler = createImportGlueHandler(sparkEngine, sqliteService);
  const connectionHandlers = createConnectionHandlers(sparkEngine, sqliteService);
  const saveAsTableHandler = createSaveAsTableHandler(config, sparkEngine, sqliteService);
  const tableDependenciesHandler = createTableDependenciesHandler(sqliteService);
  const tableDependentsHandler = createTableDependentsHandler(sqliteService);
  const listBranchesHandler = createListBranchesHandler(sqliteService);
  const listSnapshotsHandler = createListSnapshotsHandler(sparkEngine, sqliteService);
  const aiChatHandlers = createAiChatHandlers(sqliteService);

  // 6b. AI management handlers (skills, commands, memory)
  const skillHandlers = createSkillHandlers();
  const commandHandlers = createCommandHandlers();
  const memoryHandlers = createMemoryHandlers();

  // 6c. Ollama / local LLM config
  let currentOllamaPort = parseInt(process.env['NILE_OLLAMA_PORT'] ?? String(config.ai.localPort ?? 11434), 10);
  const getOllamaPort = () => currentOllamaPort;

  // 7. Register all routes from shared contract
  const implementedRoutes: Record<string, RequestHandler> = {
    'ExecuteQuery': queryExecuteHandler,
    'GetQueryStatus': queryStatusHandler,
    'GetQueryResults': queryResultsHandler,
    'StopQuery': queryStopHandler,
    'GetConfig': configHandler,
    'ListDatabases': databaseListHandler,
    'GetDatabase': databaseGetHandler,
    'ListTables': tableListHandler,
    'GetTable': tableGetHandler,
    'GetTableSchema': tableSchemaHandler,
    'SearchCatalog': searchHandler,
    'CreateDatabase': databaseCreateHandler,
    'DeleteDatabase': databaseDeleteHandler,
    'CreateTable': tableCreateHandler,
    'DeleteTable': tableDeleteHandler,
    'DetectS3Schema': importDetectHandler,
    'ImportFromPaste': importPasteHandler,
    'ImportFromS3': importS3Handler,
    'ImportFromPostgres': importPostgresHandler,
    'ImportFromSnowflake': importSnowflakeHandler,
    'ImportFromGlue': importGlueHandler,
    // Connection management
    ...connectionHandlers,
    'SaveQueryAsTable': saveAsTableHandler,
    'GetTableDependencies': tableDependenciesHandler,
    'GetTableDependents': tableDependentsHandler,
    'ListBranches': listBranchesHandler,
    'ListSnapshots': listSnapshotsHandler,
    // AI Chat session CRUD — /ai-chat/sessions routes
    'ListSessions': aiChatHandlers.listSessions,
    'GetSession': aiChatHandlers.getSession,
    'DeleteSession': aiChatHandlers.deleteSession,
    'StarSession': aiChatHandlers.starSession,
    'GetCompactedRanges': aiChatHandlers.getCompactedRanges,
    'SaveCompactedRange': aiChatHandlers.saveCompactedRange,
    // AI Chat session CRUD — /ai/sessions routes (used by desktop client)
    'ListAiSessions': aiChatHandlers.listSessions,
    'GetAiSession': aiChatHandlers.getSession,
    'DeleteAiSession': aiChatHandlers.deleteSession,
    'StarAiSession': aiChatHandlers.starSession,
    'UpdateAiSession': aiChatHandlers.updateSession,
    'GenerateAiSessionTitle': aiChatHandlers.generateTitle,
    'GetAiCompactedRanges': aiChatHandlers.getCompactedRanges,
    'SaveAiCompactedRange': aiChatHandlers.saveCompactedRange,
    // AI Skills CRUD
    'ListSkills': skillHandlers.listSkills,
    'GetSkill': skillHandlers.getSkill,
    'CreateSkill': skillHandlers.createSkill,
    'UpdateSkill': skillHandlers.updateSkill,
    'DeleteSkill': skillHandlers.deleteSkill,
    // AI Commands CRUD
    'ListCommands': commandHandlers.listCommands,
    'GetCommand': commandHandlers.getCommand,
    'CreateCommand': commandHandlers.createCommand,
    'UpdateCommand': commandHandlers.updateCommand,
    'DeleteCommand': commandHandlers.deleteCommand,
    // AI Memory
    'GetMemory': memoryHandlers.getMemory,
    'UpdateMemory': memoryHandlers.updateMemory,
    // Local LLM chat (proxies to Ollama)
    'ChatInvoke': createChatInvokeHandler(getOllamaPort, getActiveModel),
    // Visual generation (proxies to Ollama) & full-data aggregation (Spark)
    'GenerateVisuals': createGenerateVisualsHandler(getOllamaPort, getActiveModel),
    'ExecuteVisualFullData': createExecuteVisualFullDataHandler(sparkEngine),
  };

  for (const route of ROUTES) {
    const method = route.method.toLowerCase() as 'get' | 'post' | 'put' | 'delete';
    const expressPath = route.path.replace(/\{(\w+)\}/g, ':$1');
    const handler = implementedRoutes[route.id] ?? notImplementedHandler(route.id);
    app[method](expressPath, handler);
  }

  // 8. Register local-only routes (not in TypeSpec contract)
  app.post('/import/local-file', localFileImportHandler);
  app.post('/export/chart-image', createChartImageHandler(dataDir));
  app.post('/chat/invoke-streaming', createChatInvokeStreamingHandler(getOllamaPort, getActiveModel));
  app.post('/ollama/port', (req, res) => {
    const port = Number((req.body as { port?: unknown } | undefined)?.port);
    if (!Number.isInteger(port) || port < 1 || port > 65535) {
      res.status(400).json({ error: 'Invalid Ollama port' });
      return;
    }
    currentOllamaPort = port;
    res.json({ ok: true, port: currentOllamaPort });
  });

  // Helper: get the currently loaded model from Ollama (dynamic, not stale startup value).
  /** Get the model currently loaded in Ollama VRAM. Returns null if nothing loaded. */
  async function getLoadedModels(): Promise<string[]> {
    try {
      const resp = await fetch(`http://localhost:${getOllamaPort()}/api/ps`);
      const data = await resp.json() as { models?: Array<{ name: string }> };
      return (data.models || []).map((model) => model.name).filter(Boolean);
    } catch {
      return [];
    }
  }

  async function getActiveModel(): Promise<string | null> {
    try {
      const models = await getLoadedModels();
      return models[0] ?? null;
    } catch {
      return null;
    }
  }

  // Ollama model management (used by Settings UI)
  app.post('/ollama/stop-model', async (_req, res) => {
    try {
      const models = await getLoadedModels();
      for (const model of models) {
        await fetch(`http://localhost:${getOllamaPort()}/api/generate`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ model, prompt: '', keep_alive: '0', stream: false }),
        });
      }
      res.json({ ok: true });
    } catch (err) {
      res.status(502).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  // Lightweight warmup: load model into VRAM without generating a full response.
  // Uses the currently loaded model (or startup default) so it works after model changes.
  app.post('/ollama/warmup', async (_req, res) => {
    try {
      const model = await getActiveModel();
      if (!model) { res.json({ ok: true }); return; }
      await fetch(`http://localhost:${getOllamaPort()}/api/generate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ model, prompt: '', keep_alive: '30m', stream: false }),
      });
      res.json({ ok: true });
    } catch (err) {
      res.status(502).json({ error: err instanceof Error ? err.message : String(err) });
    }
  });

  // List installed Ollama models for the model selector UI.
  // Tries Ollama /api/tags first; falls back to reading ~/.ollama/models/manifests/ on disk.
  app.get('/ollama/models', async (_req, res) => {
    // Try running Ollama first
    try {
      const resp = await fetch(`http://localhost:${getOllamaPort()}/api/tags`, { signal: AbortSignal.timeout(2000) });
      const data = await resp.json() as { models?: Array<{ name: string; size: number; details?: { parameter_size?: string; quantization_level?: string } }> };
      const installed = (data.models || []).map(m => ({
        name: m.name,
        size: m.size,
        parameterSize: m.details?.parameter_size,
        quantization: m.details?.quantization_level,
      }));
      if (installed.length > 0) {
        res.json({ installed });
        return;
      }
    } catch { /* Ollama not running — fall through to filesystem */ }

    // Fallback: scan ~/.ollama/models/manifests for downloaded models.
    // Structure: library/{model}/{tag} — e.g., gemma3/27b, gemma4/latest.
    // Walk recursively so nested model paths keep showing up even when Ollama
    // isn't running and /api/tags is unavailable.
    try {
      const { readdirSync } = await import('fs');
      const manifestDir = join(homedir(), '.ollama', 'models', 'manifests', 'registry.ollama.ai', 'library');
      const installed: Array<{ name: string; size: number }> = [];

      const walkManifestDir = (dir: string, segments: string[] = []) => {
        const entries = readdirSync(dir, { withFileTypes: true });
        for (const entry of entries) {
          if (entry.isDirectory()) {
            walkManifestDir(join(dir, entry.name), [...segments, entry.name]);
            continue;
          }

          if (!entry.isFile() || segments.length === 0) {
            continue;
          }

          const modelName = segments.join('/');
          const name = entry.name === 'latest' ? modelName : `${modelName}:${entry.name}`;
          if (!installed.some((model) => model.name === name)) {
            installed.push({ name, size: 0 });
          }
        }
      };

      walkManifestDir(manifestDir);
      installed.sort((left, right) => left.name.localeCompare(right.name));
      res.json({ installed });
    } catch {
      res.json({ installed: [] });
    }
  });

  app.get('/ollama/status', async (_req, res) => {
    try {
      const port = getOllamaPort();
      const resp = await fetch(`http://localhost:${port}/api/ps`);
      const data = await resp.json() as { models?: Array<{ name: string }> };
      const models = data.models || [];
      res.json({
        running: true,
        port,
        model: models[0]?.name || null,
        models: models.map((entry) => entry.name),
        loadedModels: models.length,
      });
    } catch {
      res.json({ running: false, port: getOllamaPort(), model: null, models: [], loadedModels: 0 });
    }
  });

  // 9. Register /health outside the ROUTES loop (local-only endpoint)
  const healthHandler = createHealthHandler(config, sparkEngine, sqliteService);
  app.get('/health', healthHandler);

  // 10. Start server
  const server = app.listen(PORT, () => {
    console.log(`[api-local] Server ready. Spark: ${sparkEngine.status()}`);

    // 10a. Init Spark in background AFTER server is listening.
    // This allows the /health endpoint to report progress during init.
    if (config.compute.sparkEnabled) {
      void (async () => {
        try {
          await sparkEngine.init();
          console.log(`[api-local] Spark engine initialized (${sparkEngine.status()})`);
          if (sparkEngine.status() === 'ready') {
            await seedDefaultDatabase(sparkEngine, sqliteService, config.local.teamName);
            await purgeUnauthorizedDatabases(sparkEngine, sqliteService, config.local.teamName);
          }
        } catch (err) {
          console.warn('[api-local] Spark engine unavailable:', err);
        }
      })();
    } else {
      sparkEngine.disable('Spark is not enabled. Podman installer may be missing from the app bundle.');
      console.log('[api-local] Spark disabled (sparkEnabled=false)');
    }
  });

  // 11. Graceful shutdown
  const shutdown = async () => {
    console.log('[api-local] Shutting down...');

    // Unload Ollama model from VRAM (frees GPU memory for external Ollama instances)
    try {
      const model = await getActiveModel();
      if (!model) throw new Error('no model loaded');
      await fetch(`http://localhost:${getOllamaPort()}/api/generate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ model, prompt: '', keep_alive: '0', stream: false }),
        signal: AbortSignal.timeout(3000),
      });
      console.log('[api-local] Ollama model unloaded from VRAM.');
    } catch {
      // Ollama may already be gone -- that's fine
    }

    server.close();
    sqliteService.close();
    await queryWorkerPool.shutdown();
    await sparkEngine.shutdown();
    console.log('[api-local] Shutdown complete.');
    process.exit(0);
  };

  process.on('SIGTERM', () => void shutdown());
  process.on('SIGINT', () => void shutdown());
}

/**
 * Seeds the team database on first run if no databases exist yet.
 * Uses Spark to create the Iceberg namespace.
 */
async function seedDefaultDatabase(
  sparkEngine: SparkEngine,
  sqliteService: SQLiteService,
  teamName: string,
): Promise<void> {
  const existing = sqliteService.listDatabases();
  if (existing.length > 0) {
    console.log(`[api-local] Catalog already has ${existing.length} database(s), skipping seed.`);
    return;
  }
  console.log(`[api-local] First run -- seeding default database: "${teamName}"`);
  await sparkEngine.executeDDL(`CREATE NAMESPACE IF NOT EXISTS ${teamName}`);
  sqliteService.insertDatabase(teamName, `Default database for team ${teamName}`, teamName);
  console.log(`[api-local] Default database "${teamName}" ready.`);
}

/**
 * Seeds the team database on first run without Spark (SQLite-only).
 * Used when Spark is not available at startup.
 */
function seedDefaultDatabaseFallback(
  sqliteService: SQLiteService,
  teamName: string,
): void {
  const existing = sqliteService.listDatabases();
  if (existing.length > 0) {
    console.log(`[api-local] Catalog already has ${existing.length} database(s), skipping seed.`);
    return;
  }
  console.log(`[api-local] First run -- seeding default database (SQLite only): "${teamName}"`);
  sqliteService.insertDatabase(teamName, `Default database for team ${teamName}`, teamName);
  console.log(`[api-local] Default database "${teamName}" ready (Iceberg namespace will be created when Spark is available).`);
}

/**
 * Removes unauthorized databases from SQLite catalog only (no Spark needed).
 * Called at startup before Spark is available.
 */
function purgeUnauthorizedDatabasesCatalogOnly(
  sqliteService: SQLiteService,
  teamName: string,
): void {
  const databases = sqliteService.listDatabases();
  for (const db of databases) {
    if (db.name !== teamName) {
      console.log(`[api-local] Purging unauthorized database "${db.name}" from catalog (only "${teamName}" is allowed)`);
      sqliteService.deleteDatabase(db.name);
    }
  }
}

/**
 * Removes databases that don't match the allowed team name.
 * Cleans up any stale databases (e.g. "nile") from both Spark and SQLite catalog.
 */
async function purgeUnauthorizedDatabases(
  sparkEngine: SparkEngine,
  sqliteService: SQLiteService,
  teamName: string,
): Promise<void> {
  const databases = sqliteService.listDatabases();
  for (const db of databases) {
    if (db.name !== teamName) {
      console.log(`[api-local] Purging unauthorized database "${db.name}" (only "${teamName}" is allowed)`);
      try {
        await sparkEngine.executeDDL(`DROP NAMESPACE IF EXISTS ${db.name} CASCADE`);
      } catch {
        // Ignore Spark errors — namespace may not exist
      }
      sqliteService.deleteDatabase(db.name);
      console.log(`[api-local] Database "${db.name}" removed.`);
    }
  }
}

function notImplementedHandler(routeId: string): RequestHandler {
  return (_req, res) => {
    res.status(501).json({
      error: 'NOT_AVAILABLE_IN_LOCAL_MODE',
      message: `${routeId} is not available in local mode. Use Nile cloud for this feature.`,
      routeId,
    });
  };
}

void main();
