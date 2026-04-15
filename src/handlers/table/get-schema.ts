import type { RequestHandler } from 'express';
import type { SparkEngine } from '../../engines/spark-engine.js';
import type { Column } from '../../types/types.js';

export function createTableSchemaHandler(sparkEngine: SparkEngine): RequestHandler {
  return async (req, res) => {
    const { database, table } = req.params as { database: string; table: string };
    try {
      const result = await sparkEngine.executeSQL(`DESCRIBE TABLE ${database}.${table}`);
      const columns: Column[] = result.rows.map(row => ({
        name: String(row['col_name']),
        dataType: String(row['data_type']),
        nullable: true,
      }));
      res.json(columns);
    } catch {
      res.status(404).json({
        error: 'NOT_FOUND',
        message: `Table '${database}.${table}' not found or cannot be described.`,
      });
    }
  };
}
