import { userInfo } from 'os';
import type { RequestHandler } from 'express';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { ListBranchesResponse, BranchInfo } from '../../types/types.js';

const LOCAL_USER = userInfo().username;

/**
 * ListBranches handler for local mode.
 * Local mode has a single branch per table: v1.main (no branching support).
 */
export function createListBranchesHandler(sqliteService: SQLiteService): RequestHandler {
  return (req, res) => {
    const { database, table } = req.params as { database: string; table: string };
    const row = sqliteService.getTable(database, table);
    if (!row) {
      res.status(404).json({ error: 'NOT_FOUND', message: `Table '${database}.${table}' not found.` });
      return;
    }

    const branch: BranchInfo = {
      branchName: 'v1.main',
      database,
      table,
      version: 'v1.0.0',
      createdOn: row.createdAt,
      createdBy: LOCAL_USER,
      status: 'active',
      isDefault: true,
      isPrimary: true,
      versionInfo: { major: 1, minor: 0, build: 0 },
      lastCommitTimestamp: row.updatedAt || row.createdAt,
      commitCount: 1,
      description: 'Main branch (local mode)',
    };

    const response: ListBranchesResponse = { branches: [branch] };
    res.json(response);
  };
}
