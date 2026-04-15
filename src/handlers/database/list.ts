import type { RequestHandler } from 'express';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { DatabaseInfo } from '../../types/types.js';

export function createDatabaseListHandler(sqliteService: SQLiteService): RequestHandler {
  return (req, res) => {
    const teamFilter = req.query['team'] as string | undefined;
    const rows = sqliteService.listDatabases(teamFilter);
    const databases: (DatabaseInfo & { teamName?: string })[] = rows.map((r) => ({
      name: r.name,
      description: r.description,
      teamName: r.teamName,
      createdAt: r.createdAt,
    }));
    res.json({ databases });
  };
}
