import type { RequestHandler } from 'express';
import type { SparkEngine } from '../../engines/spark-engine.js';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { DeleteTableResponse } from '../../types/types.js';

export function createTableDeleteHandler(sparkEngine: SparkEngine, sqliteService: SQLiteService): RequestHandler {
  return async (req, res) => {
    const { database, table } = req.params as { database: string; table: string };
    try {
      await sparkEngine.executeDDL(`DROP TABLE IF EXISTS ${database}.${table}`);
    } catch {
      // Ignore if table didn't exist in Spark/Iceberg
    }
    sqliteService.deleteTable(database, table);
    const body: DeleteTableResponse = {
      success: true,
      message: `Table ${database}.${table} deleted`,
    };
    res.json(body);
  };
}
