/**
 * esbuild Configuration for api-local Bundling
 *
 * Bundles api-local into a single JS file with all dependencies included.
 * This is required for Tauri production builds where node_modules is not bundled.
 *
 * Usage:
 *   npm run build:bundle
 *
 * Output:
 *   dist/bundle/server.js - Single bundled file for production
 *
 * Note: Spark files (Dockerfile, spark_sidecar.py, entrypoint.sh) are NOT bundled
 * here — they are copied separately via tauri.conf.json resources.
 */

import * as esbuild from 'esbuild';
import { fileURLToPath } from 'url';
import { dirname as pathDirname, join } from 'path';

const __dirname = pathDirname(fileURLToPath(import.meta.url));

await esbuild.build({
  entryPoints: [join(__dirname, 'dist/server.js')],
  bundle: true,
  platform: 'node',
  target: 'node22',
  format: 'esm',
  outfile: join(__dirname, 'dist/bundle/server.js'),
  sourcemap: true,

  // No external deps — everything is pure JS and bundleable
  // Node 20 built-in sqlite (node:sqlite) doesn't need bundling
  external: [],

  // Handle Node.js built-in modules
  // NOTE: Do NOT import fileURLToPath/dirname here — esbuild hoists these
  // from source files, causing duplicate ESM binding errors at runtime.
  // Only inject createRequire (CJS compat) and __filename/__dirname helpers
  // using dynamic expressions that don't conflict with hoisted imports.
  banner: {
    js: `
import { createRequire as __banner_createRequire } from 'module';
const require = __banner_createRequire(import.meta.url);
`.trim(),
  },

  // Define environment for bundling
  define: {
    'process.env.NODE_ENV': '"production"',
  },

  // Log output
  logLevel: 'info',
});

console.log('✓ api-local bundled to dist/bundle/server.js');
