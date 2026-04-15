import type { RequestHandler } from 'express';
import type { SparkEngine } from '../../engines/spark-engine.js';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { LocalConfig } from '../../config/environment.js';

export function createDatabaseDeleteHandler(config: LocalConfig, sparkEngine: SparkEngine, sqliteService: SQLiteService): RequestHandler {
  return async (req, res) => {
    const { database } = req.params as { database: string };
    const teamDb = config.local.teamName;
    if (database === teamDb) {
      res.status(403).json({
        error: 'DATABASE_PROTECTED',
        message: `The team database "${teamDb}" cannot be deleted.`,
      });
      return;
    }
    try {
      await sparkEngine.executeDDL(`DROP NAMESPACE IF EXISTS ${database} CASCADE`);
    } catch {
      // Ignore if namespace didn't exist in Spark/Iceberg
    }
    sqliteService.deleteDatabase(database);
    res.status(204).send();
  };
}
