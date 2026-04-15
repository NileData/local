import type { RequestHandler } from 'express';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { DatabaseInfo } from '../../types/types.js';

export function createDatabaseGetHandler(sqliteService: SQLiteService): RequestHandler {
  return (req, res) => {
    const { database } = req.params as { database: string };
    const row = sqliteService.getDatabase(database);
    if (!row) {
      res.status(404).json({ error: 'NOT_FOUND', message: `Database '${database}' not found.` });
      return;
    }
    const db: DatabaseInfo = { name: row.name, description: row.description, createdAt: row.createdAt };
    res.json({ database: db });
  };
}
