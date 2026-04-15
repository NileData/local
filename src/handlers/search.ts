import type { RequestHandler } from 'express';
import type { SQLiteService } from '../services/sqlite-service.js';
import type { SearchResultItem } from '../types/types.js';
import { MatchType } from '../types/types.js';

export function createSearchHandler(sqliteService: SQLiteService): RequestHandler {
  return (req, res) => {
    const query = (req.query['q'] as string | undefined)
      ?? (req.query['searchQuery'] as string | undefined)
      ?? '';
    const limit = parseInt(req.query['limit'] as string ?? '20', 10);

    if (!query.trim()) {
      res.json({ query: '', totalResults: 0, searchTimeMs: 0, results: [] });
      return;
    }

    const startTime = Date.now();
    const rows = sqliteService.listTables();
    const lower = query.toLowerCase();

    const results: SearchResultItem[] = rows
      .filter((r) =>
        r.tableName.toLowerCase().includes(lower) ||
        r.databaseName.toLowerCase().includes(lower) ||
        (r.description ?? '').toLowerCase().includes(lower)
      )
      .slice(0, limit)
      .map((r) => {
        // Determine which field matched for matchField
        let matchField = 'tableName';
        if (r.tableName.toLowerCase().includes(lower)) {
          matchField = 'tableName';
        } else if (r.databaseName.toLowerCase().includes(lower)) {
          matchField = 'fullTableName';
        } else {
          matchField = 'description';
        }

        return {
          resultType: 'table' as const,
          database: r.databaseName,
          tableName: r.tableName,
          fullTableName: `${r.databaseName}.${r.tableName}`,
          description: r.description,
          matchField,
          matchType: MatchType.Contains,
          score: 1.0,
        };
      });

    const searchTimeMs = Date.now() - startTime;
    res.json({ query, totalResults: results.length, searchTimeMs, results });
  };
}
