import type { RequestHandler } from 'express';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { GetTableDependentsResponse, DependentTable } from '../../types/types.js';

export function createTableDependentsHandler(sqliteService: SQLiteService): RequestHandler {
  return (req, res) => {
    const { database, table } = req.params as { database: string; table: string };
    const targetKey = `${database}.${table}`;

    // Scan all tables and find those that declare or detect this table as a dependency
    const allTables = sqliteService.listTables();
    const dependents: DependentTable[] = [];

    for (const row of allTables) {
      if (!row.definitionJson) continue;
      try {
        const def = JSON.parse(row.definitionJson) as Record<string, unknown>;
        const deps = def.dependencies as { tables?: { declared?: Array<{ database: string; table: string }>; actual?: Array<{ database: string; table: string }> } } | undefined;
        if (!deps?.tables) continue;

        const allDeps = [...(deps.tables.declared || []), ...(deps.tables.actual || [])];
        const references = allDeps.some(d => `${d.database}.${d.table}` === targetKey);
        if (references) {
          dependents.push({
            database: row.databaseName,
            tableName: row.tableName,
          });
        }
      } catch {
        // Skip unparseable definitions
      }
    }

    const response: GetTableDependentsResponse = { dependents };
    res.json(response);
  };
}
