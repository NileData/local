/**
 * POST /query/visuals — Generate visualization plan via local LLM (Ollama).
 *
 * Generates visualization plans using Ollama's Anthropic-compatible
 * /v1/messages endpoint instead of Bedrock.
 */

import { createHash } from 'crypto';
import type { RequestHandler } from 'express';
import { VISUAL_GENERATION_PROMPT, VISUAL_TEMPERATURE } from '../../lib/shared-config/index.js';
import type {
  GenerateVisualsRequest,
  GenerateVisualsResponse,
  VisualConfig,
  DataMetrics,
} from '../../types/types.js';

interface VisualPlan {
  visualizations: VisualConfig[];
  dataMetrics?: DataMetrics;
}

// ---------------------------------------------------------------------------
// User message builder (visual generation helpers)
// ---------------------------------------------------------------------------

function buildUserMessage(
  sql: string,
  data: unknown[][],
  schema: string[][],
  userQuestion: string | undefined,
  maxVisualizations: number,
): string {
  const parts: string[] = [];

  parts.push('## Executed SQL Query');
  parts.push('```sql');
  parts.push(sql);
  parts.push('```');
  parts.push('');

  parts.push('## Result Schema');
  parts.push('| Column | Type |');
  parts.push('|--------|------|');
  for (const [name, type] of schema) {
    parts.push(`| ${name} | ${type} |`);
  }
  parts.push('');

  const sampleRows = data.slice(0, 100);
  parts.push(`## Sample Data (${sampleRows.length} of ${data.length} rows)`);
  parts.push('```json');
  parts.push(JSON.stringify(sampleRows.slice(0, 20), null, 2));
  parts.push('```');
  parts.push('');

  if (userQuestion) {
    parts.push('## Original User Question');
    parts.push(userQuestion);
    parts.push('');
  }

  parts.push('## Configuration');
  parts.push(`- Maximum visualizations: ${maxVisualizations}`);
  parts.push(`- Total rows in sample: ${data.length}`);

  return parts.join('\n');
}

// ---------------------------------------------------------------------------
// Response parsing & validation (visual generation helpers)
// ---------------------------------------------------------------------------

function parseVisualPlan(content: string): VisualPlan {
  const jsonMatch = content.match(/```json\n?([\s\S]*?)\n?```/) || content.match(/(\{[\s\S]*\})/);
  if (!jsonMatch) {
    throw new Error('Failed to parse visual plan from model response');
  }
  try {
    return JSON.parse(jsonMatch[1]);
  } catch {
    throw new Error('Failed to parse visual plan JSON');
  }
}

function computeDataMetrics(schema: string[][], rowCount: number): DataMetrics {
  const numericTypes = ['int', 'bigint', 'double', 'float', 'decimal', 'numeric', 'integer', 'real', 'long'];
  const dateTypes = ['date', 'time', 'timestamp'];

  const numericColumns: string[] = [];
  const categoricalColumns: string[] = [];
  const dateColumns: string[] = [];

  for (const [name, type] of schema) {
    const lower = type.toLowerCase();
    if (numericTypes.some(t => lower.includes(t))) {
      numericColumns.push(name);
    } else if (dateTypes.some(t => lower.includes(t))) {
      dateColumns.push(name);
    } else {
      categoricalColumns.push(name);
    }
  }

  return { rowCount, numericColumns, categoricalColumns, dateColumns };
}

function validateAndEnrichPlan(plan: VisualPlan, schema: string[][], rowCount: number): VisualPlan {
  if (!plan.visualizations || !Array.isArray(plan.visualizations)) {
    plan.visualizations = [];
  }

  plan.visualizations = plan.visualizations.map((viz, index) => ({
    id: viz.id || `viz-${index}`,
    title: viz.title || `Visualization ${index + 1}`,
    vizType: viz.vizType || 'table',
    xAxis: viz.xAxis || schema[0]?.[0] || 'column1',
    yAxis: viz.yAxis,
    yAxisColumns: viz.yAxisColumns,
    groupBy: viz.groupBy,
    aggregation: viz.aggregation,
    description: viz.description,
    options: viz.options,
  }));

  if (!plan.dataMetrics) {
    plan.dataMetrics = computeDataMetrics(schema, rowCount);
  } else {
    plan.dataMetrics.rowCount = rowCount;
  }

  return plan;
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

export function createGenerateVisualsHandler(
  getOllamaPort: () => number,
  getActiveModel: () => Promise<string | null>,
): RequestHandler {
  return async (req, res) => {
    const body = req.body as GenerateVisualsRequest | undefined;

    if (!body?.executedSql) {
      res.status(400).json({ error: 'VALIDATION_ERROR', message: 'Missing required field: executedSql' });
      return;
    }
    if (!body.sampleData || !Array.isArray(body.sampleData)) {
      res.status(400).json({ error: 'VALIDATION_ERROR', message: 'Missing required field: sampleData' });
      return;
    }
    if (!body.schema || !Array.isArray(body.schema)) {
      res.status(400).json({ error: 'VALIDATION_ERROR', message: 'Missing required field: schema' });
      return;
    }

    // Check AI model availability (same pattern as chat invoke.ts)
    const model = await getActiveModel();
    if (!model) {
      res.status(503).json({ error: 'NO_MODEL_LOADED', message: 'No model loaded. Open Settings > AI Assistant and run a model first.' });
      return;
    }

    try {
      const { executedSql, sampleData, schema, userQuestion, maxVisualizations = 3 } = body;
      const ollamaPort = getOllamaPort();

      const userMessage = buildUserMessage(executedSql, sampleData, schema, userQuestion, maxVisualizations);

      const response = await fetch(`http://localhost:${ollamaPort}/v1/messages`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-api-key': 'ollama',
          'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify({
          model,
          max_tokens: 2048,
          temperature: VISUAL_TEMPERATURE,
          system: VISUAL_GENERATION_PROMPT,
          messages: [{ role: 'user', content: userMessage }],
        }),
      });

      if (!response.ok) {
        const errorText = await response.text().catch(() => response.statusText);
        throw new Error(`LLM request failed (${response.status}): ${errorText}`);
      }

      const result = await response.json() as { content?: Array<{ type: string; text?: string }> };
      const content = result.content?.[0]?.text;
      if (!content) {
        throw new Error('No content in model response');
      }

      const plan = parseVisualPlan(content);
      const enriched = validateAndEnrichPlan(plan, schema, sampleData.length);

      const cacheKey = createHash('sha256')
        .update(`${executedSql}|${sampleData.length}|${userQuestion || ''}`)
        .digest('hex')
        .slice(0, 16);

      const resp: GenerateVisualsResponse = {
        visualizations: enriched.visualizations,
        dataMetrics: enriched.dataMetrics,
        cacheKey,
      };

      res.json(resp);
    } catch (err) {
      console.error('[generate-visuals] Error:', err instanceof Error ? err.message : String(err));
      res.status(500).json({
        error: 'VISUAL_GENERATION_FAILED',
        message: err instanceof Error ? err.message : 'Failed to generate visuals',
      });
    }
  };
}
