import type { RequestHandler } from 'express';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { TableCatalogEntry } from '../../types/types.js';

export function createTableListHandler(sqliteService: SQLiteService): RequestHandler {
  return (req, res) => {
    const database = req.query['database'] as string | undefined;
    const rows = sqliteService.listTables(database);
    const includeSchema = req.query['includeSchema'] === 'true';
    const tables: TableCatalogEntry[] = rows.map((r) => {
      const entry: TableCatalogEntry = {
        database: r.databaseName,
        tableName: r.tableName,
        fullTableName: `${r.databaseName}.${r.tableName}`,
        description: r.description,
        tableType: 'MANAGED',
        branch: 'v1.main',
      };
      if (includeSchema && r.schemaJson) {
        try {
          (entry as TableCatalogEntry & { schema?: unknown }).schema = JSON.parse(r.schemaJson);
        } catch {
          // Non-critical
        }
      }
      // Extract declared dependencies for catalog tree expand arrows
      if (r.definitionJson) {
        try {
          const def = JSON.parse(r.definitionJson) as { dependencies?: { tables?: { declared?: Array<{ database: string; table: string }> } } };
          const declared = def.dependencies?.tables?.declared;
          if (declared && declared.length > 0) {
            entry.declaredDependencies = declared.map(d => `${d.database}.${d.table}`);
          }
        } catch {
          // Non-critical
        }
      }
      return entry;
    });
    res.json({ tables });
  };
}
