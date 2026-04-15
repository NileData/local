import type { RequestHandler } from 'express';
import type { SparkEngine } from '../../engines/spark-engine.js';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { LocalConfig } from '../../config/environment.js';

export function createDatabaseCreateHandler(config: LocalConfig, sparkEngine: SparkEngine, sqliteService: SQLiteService): RequestHandler {
  return async (req, res) => {
    const body = req.body as Record<string, unknown> | undefined;
    const name = body?.name as string | undefined;
    if (!name || typeof name !== 'string' || name.trim() === '') {
      res.status(400).json({ error: 'VALIDATION_ERROR', message: 'Request body must include a non-empty "name" field.' });
      return;
    }
    const allowedName = config.local.teamName;
    if (name !== allowedName) {
      res.status(403).json({
        error: 'DATABASE_NAME_RESTRICTED',
        message: `Only the team database "${allowedName}" is allowed. Cannot create database "${name}".`,
      });
      return;
    }
    try {
      await sparkEngine.executeDDL(`CREATE NAMESPACE IF NOT EXISTS ${name}`);
      sqliteService.insertDatabase(name, body?.description as string | undefined, config.local.teamName);
      const row = sqliteService.getDatabase(name);
      res.status(201).json({ database: { name: row?.name ?? name, description: row?.description, teamName: row?.teamName, createdAt: row?.createdAt } });
    } catch (err) {
      res.status(500).json({ error: 'INTERNAL_ERROR', message: err instanceof Error ? err.message : String(err) });
    }
  };
}
