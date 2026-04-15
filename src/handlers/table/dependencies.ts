import type { RequestHandler } from 'express';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { GetTableDependenciesResponse, DependencyInfo } from '../../types/types.js';

export function createTableDependenciesHandler(sqliteService: SQLiteService): RequestHandler {
  return (req, res) => {
    const { database, table } = req.params as { database: string; table: string };
    const row = sqliteService.getTable(database, table);
    if (!row) {
      res.status(404).json({ error: 'NOT_FOUND', message: `Table '${database}.${table}' not found.` });
      return;
    }

    // Extract dependencies from stored definition
    let dependencies: DependencyInfo = { tables: { declared: [], actual: [] } };
    if (row.definitionJson) {
      try {
        const def = JSON.parse(row.definitionJson) as Record<string, unknown>;
        if (def.dependencies) {
          dependencies = def.dependencies as DependencyInfo;
        }
      } catch {
        // Fall through to empty dependencies
      }
    }

    const response: GetTableDependenciesResponse = { dependencies };
    res.json(response);
  };
}
