/**
 * Chart Image Export Handler
 *
 * @experimental This handler currently returns a placeholder SVG.
 * Full chart rendering requires satori + @resvg/resvg-js + chart-themes.
 *
 * POST /export/chart-image
 *
 * Renders a chart as a PNG image using Satori (JSX -> SVG) + Resvg (SVG -> PNG).
 * Returns the file path to the generated PNG in ~/.nile/operations/results/.
 */

import type { RequestHandler } from 'express';
import { join } from 'path';
import { writeFile, mkdir } from 'fs/promises';
import { randomUUID } from 'crypto';

export interface ChartImageInput {
  vizType: 'bar' | 'line' | 'pie' | 'scatter' | 'area' | 'histogram' | 'boxplot';
  title: string;
  data: Record<string, unknown>[];
  xAxis: string;
  yAxis?: string;
  yAxisColumns?: string[];
  groupBy?: string;
  options?: {
    layout?: 'horizontal' | 'vertical';
    stacked?: boolean;
    curved?: boolean;
    donut?: boolean;
  };
}

interface ChartImageResponse {
  filePath: string;
  fileName: string;
}

interface ChartImageErrorResponse {
  error: string;
}

/**
 * Create the chart image export handler.
 *
 * @param dataDir - Base data directory (~/.nile)
 */
export function createChartImageHandler(
  dataDir: string
): RequestHandler {
  return async (req, res) => {
    try {
      const input = req.body as ChartImageInput;

      if (!input.vizType || !input.title || !input.data || !input.xAxis) {
        res.status(400).json({
          error: 'Missing required fields: vizType, title, data, xAxis',
        } satisfies ChartImageErrorResponse);
        return;
      }

      const resultsDir = join(dataDir, 'operations', 'results');
      await mkdir(resultsDir, { recursive: true });

      const fileName = `chart-${randomUUID().slice(0, 8)}.png`;
      const filePath = join(resultsDir, fileName);

      // Render chart to PNG
      const pngBuffer = await renderChartToPng(input);
      await writeFile(filePath, pngBuffer);

      res.json({
        filePath,
        fileName,
      } satisfies ChartImageResponse);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      res.status(500).json({
        error: `Chart export failed: ${message}`,
      } satisfies ChartImageErrorResponse);
    }
  };
}

/**
 * Render a chart to SVG buffer.
 *
 * Currently generates a styled SVG chart placeholder. When satori + @resvg/resvg-js
 * are added to api-local dependencies, this can be upgraded to render full PNG charts
 * using full server-side chart rendering components.
 *
 * TODO: Add satori + @resvg/resvg-js + react deps and port ServerChart components
 * for full visual parity with client-side Recharts rendering.
 */
async function renderChartToPng(input: ChartImageInput): Promise<Buffer> {
  const svg = buildChartSvg(input);
  return Buffer.from(svg, 'utf-8');
}

/**
 * Build an SVG representation of the chart.
 */
function buildChartSvg(input: ChartImageInput): string {
  const escaped = input.title.replace(/[<>&"]/g, c => ({
    '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;'
  }[c] || c));

  return `<svg xmlns="http://www.w3.org/2000/svg" width="800" height="500" viewBox="0 0 800 500">
  <rect width="800" height="500" fill="#1e1e2e"/>
  <text x="400" y="230" text-anchor="middle" fill="#cdd6f4" font-size="24" font-family="sans-serif">${escaped}</text>
  <text x="400" y="270" text-anchor="middle" fill="#6c7086" font-size="14" font-family="sans-serif">${input.vizType} chart | ${input.data.length} data points</text>
  <text x="400" y="310" text-anchor="middle" fill="#45475a" font-size="12" font-family="sans-serif">Install satori + @resvg/resvg-js for full chart rendering</text>
</svg>`;
}
