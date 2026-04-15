/**
 * POST /query/visuals/full-data — Execute aggregation query for full-data visualizations.
 *
 * Runs an aggregation SQL query on Spark (no LLM needed) and returns
 * the aggregated results for chart rendering.
 */

import type { RequestHandler } from 'express';
import type { SparkEngine } from '../../engines/spark-engine.js';
import type {
  ExecuteVisualFullDataRequest,
  ExecuteVisualFullDataResponse,
  AggregationConfig,
} from '../../types/types.js';

// ---------------------------------------------------------------------------
// SQL builders for aggregation queries
// ---------------------------------------------------------------------------

function buildCountSql(originalSql: string): string {
  const baseSql = originalSql.replace(/\s+LIMIT\s+\d+/gi, '');
  return `SELECT COUNT(*) AS total_count FROM (${baseSql}) AS _count_subq`;
}

function buildAggregationSql(originalSql: string, config: AggregationConfig): string {
  const { aggType, column, groupBy = [], sortBy, sortOrder = 'desc', limit } = config;

  const baseSql = originalSql.replace(/\s+LIMIT\s+\d+/gi, '');

  let aggFunc: string;
  switch (aggType) {
    case 'sum':      aggFunc = `SUM(${column})`; break;
    case 'avg':      aggFunc = `AVG(${column})`; break;
    case 'count':    aggFunc = `COUNT(*)`; break;
    case 'count_distinct': aggFunc = `COUNT(DISTINCT ${column})`; break;
    case 'min':      aggFunc = `MIN(${column})`; break;
    case 'max':      aggFunc = `MAX(${column})`; break;
    default: throw new Error(`Unsupported aggregation type: ${aggType}`);
  }

  const aggAlias = groupBy.includes(column) ? `${aggType}_value` : column;
  const selectCols = [...groupBy, `${aggFunc} AS ${aggAlias}`].join(', ');

  let sql = `SELECT ${selectCols} FROM (${baseSql}) AS _subq`;

  if (groupBy.length > 0) {
    sql += ` GROUP BY ${groupBy.join(', ')}`;
  }
  if (sortBy) {
    // Validate sortBy against actual output columns — LLM may generate
    // names like "average_x" that don't match the real alias.
    const outputCols = [...groupBy, aggAlias];
    const resolvedSort = outputCols.includes(sortBy) ? sortBy : aggAlias;
    sql += ` ORDER BY ${resolvedSort} ${sortOrder?.toUpperCase() || 'DESC'}`;
  }
  if (limit) {
    sql += ` LIMIT ${limit}`;
  }

  return sql;
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

export function createExecuteVisualFullDataHandler(sparkEngine: SparkEngine): RequestHandler {
  return async (req, res) => {
    const body = req.body as ExecuteVisualFullDataRequest | undefined;

    if (!body?.originalSql) {
      res.status(400).json({ error: 'VALIDATION_ERROR', message: 'Missing required field: originalSql' });
      return;
    }
    if (!body.visualId) {
      res.status(400).json({ error: 'VALIDATION_ERROR', message: 'Missing required field: visualId' });
      return;
    }
    if (!body.aggregation) {
      res.status(400).json({ error: 'VALIDATION_ERROR', message: 'Missing required field: aggregation' });
      return;
    }

    try {
      const aggregatedSql = buildAggregationSql(body.originalSql, body.aggregation);
      const countSql = buildCountSql(body.originalSql);

      // Run aggregation first, then count (sequential to avoid Spark serialization delays)
      const aggResult = await sparkEngine.executeSQL(aggregatedSql);
      const countResult = await sparkEngine.executeSQL(countSql).catch(() => null);

      // Convert to array-of-arrays format expected by the response type
      const colNames = aggResult.columns.map(c => c.name);
      const dataArrays = aggResult.rows.map(row => colNames.map(col => row[col]));
      const schemaArrays = aggResult.columns.map(c => [c.name, c.type ?? 'string']);

      let totalRowsProcessed = dataArrays.length;
      if (countResult && countResult.rows.length > 0) {
        totalRowsProcessed = Number(countResult.rows[0]?.['total_count'] ?? dataArrays.length);
      }

      const resp: ExecuteVisualFullDataResponse = {
        data: dataArrays,
        schema: schemaArrays,
        totalRowsProcessed,
        visualId: body.visualId,
      };

      res.json(resp);
    } catch (err) {
      console.error('[execute-visual-full-data] Error:', err instanceof Error ? err.message : String(err));
      res.status(500).json({
        error: 'FULL_DATA_QUERY_FAILED',
        message: err instanceof Error ? err.message : 'Failed to execute full data query',
      });
    }
  };
}
