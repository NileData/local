import { userInfo } from 'os';
import type { RequestHandler } from 'express';
import type { SparkEngine } from '../../engines/spark-engine.js';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { GetTableResponse, Column } from '../../types/types.js';

const LOCAL_USER = userInfo().username;

export function createTableGetHandler(sparkEngine: SparkEngine, sqliteService: SQLiteService): RequestHandler {
  return async (req, res) => {
    const { database, table } = req.params as { database: string; table: string };
    const row = sqliteService.getTable(database, table);
    if (!row) {
      res.status(404).json({ error: 'NOT_FOUND', message: `Table '${database}.${table}' not found.` });
      return;
    }

    // Live row count from Spark
    let rowCount: number | undefined;
    try {
      const countResult = await sparkEngine.executeSQL(`SELECT COUNT(*) AS row_count FROM ${database}.${table}`);
      rowCount = Number(countResult.rows[0]?.['row_count'] ?? 0);
    } catch {
      // Live stats optional -- don't fail the request
    }

    // Parse schema from stored JSON
    let schema: Column[] = [];
    try {
      if (row.schemaJson) {
        const parsed = JSON.parse(row.schemaJson) as Array<{ name: string; dataType: string; nullable?: boolean }>;
        schema = parsed.map(c => ({ name: c.name, dataType: c.dataType, nullable: c.nullable ?? true }));
      }
    } catch {
      // Non-critical
    }

    // Parse stored definition or build a minimal one
    let definition: Record<string, unknown> = {
      schema,
      description: row.description || '',
      updateStrategy: 'replace',
    };
    if (row.definitionJson) {
      try {
        const stored = JSON.parse(row.definitionJson) as Record<string, unknown>;
        // Merge live schema into stored definition (schema may have been updated)
        definition = { ...stored, schema, description: stored.description || row.description || '' };
      } catch {
        // Fall through to default definition
      }
    }

    const response: GetTableResponse = {
      partitionKey: database,
      sortKey: `${table}.v1.main`,
      database,
      tableName: table,
      fullTableName: `${database}.${table}`,
      tableType: row.tableType || 'MANAGED',
      version: { major: 1, minor: 0, build: 0 },
      branch: 'v1.main',
      definition: definition as GetTableResponse['definition'],
      createdOn: row.createdAt,
      updatedOn: row.updatedAt || row.createdAt,
      createdBy: LOCAL_USER,
      updatedBy: LOCAL_USER,
      metadata: rowCount !== undefined ? { rowCount } : undefined,
    };

    res.json(response);
  };
}
