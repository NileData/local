/**
 * Dependency Extraction Utilities
 *
 * Provides functions for normalizing and filtering table dependencies
 * extracted from Spark query plans. Used by:
 * - SAT wizard (save-as-table.ts) - extract on table creation
 * - Finalize success (finalize-success.ts) - recapture on ETL version change
 * - Ad-hoc queries (query-templates.ts) - inline extraction
 *
 * Raw extraction from Spark query plans produces:
 * - Table aliases like `default.p`, `default.s` (should filter)
 * - Duplicates like `sales.products` AND `spark_catalog.sales.products` (should dedupe)
 * - Temp views in `default.*` or `global_temp.*` (should filter)
 * - Real tables like `sales.customers` (should keep)
 *
 * Pure functions, zero infrastructure dependencies.
 */

/**
 * Represents an actual dependency detected from code analysis.
 * This matches the TypeSpec ActualDependency model structure.
 */
export interface ActualDependency {
  /** Database name */
  database: string;
  /** Table name */
  table: string;
  /** Confidence score from 0.0 to 1.0 */
  confidence: number;
  /** How the dependency was detected */
  detectionMethod: 'sql-parser' | 'python-ast';
}

/**
 * Raw dependency data from S3 (written by Spark inline extraction)
 */
export interface RawDependencyData {
  tables: string[];
  confidence: number;
  detectionMethod: string;
  detectedAt: string;
  error: string | null;
}

/**
 * Normalize and filter raw table references from Spark query plan extraction.
 *
 * This function:
 * 1. Removes `spark_catalog.` prefix (normalize)
 * 2. Filters out `default.*` (temp views, CTEs, table aliases)
 * 3. Filters out `global_temp.*` (global temp views)
 * 4. Filters out short table names (aliases like p, c, s, t1, t2)
 * 5. Deduplicates entries
 *
 * @param tables - Raw table references from Spark plan extraction
 * @param detectionMethod - How the dependencies were detected
 * @returns Normalized and filtered actual dependencies
 *
 * @example
 * ```typescript
 * const raw = ['default.p', 'sales.products', 'spark_catalog.sales.products'];
 * const filtered = normalizeAndFilterDeps(raw);
 * // Returns: [{ database: 'sales', table: 'products', confidence: 1.0, detectionMethod: 'sql-parser' }]
 * ```
 */
export function normalizeAndFilterDeps(
  tables: string[],
  detectionMethod: 'sql-parser' | 'python-ast' = 'sql-parser'
): ActualDependency[] {
  const seen = new Set<string>();
  const result: ActualDependency[] = [];

  for (const table of tables) {
    // Remove spark_catalog prefix (Iceberg catalog prefix)
    let clean = table.replace(/^spark_catalog\./, '');

    // Skip default database (temp views, CTEs, table aliases)
    if (clean.startsWith('default.')) {
      continue;
    }

    // Skip global_temp database (global temp views)
    if (clean.startsWith('global_temp.')) {
      continue;
    }

    // Parse database.table format
    const parts = clean.split('.');
    if (parts.length !== 2) {
      // Skip if not in database.table format
      continue;
    }

    const [db, tbl] = parts;

    // Skip short table names (likely aliases like p, c, s, t1, t2)
    // Real table names are typically at least 3 characters
    if (tbl.length <= 2) {
      continue;
    }

    // Skip if database name is too short (might be an alias)
    if (db.length <= 1) {
      continue;
    }

    // Deduplicate using normalized key
    const key = `${db}.${tbl}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);

    result.push({
      database: db,
      table: tbl,
      confidence: 1.0,
      detectionMethod,
    });
  }

  return result;
}

/**
 * Parse raw dependency JSON from S3 and normalize/filter it.
 *
 * @param rawJson - Raw JSON string from S3 actual-dependencies.json
 * @returns Normalized actual dependencies or null if parsing fails
 */
export function parseAndNormalizeDeps(rawJson: string): ActualDependency[] | null {
  try {
    const raw: RawDependencyData = JSON.parse(rawJson);

    if (raw.error) {
      console.warn('Dependency extraction had error:', raw.error);
      return null;
    }

    if (!raw.tables || !Array.isArray(raw.tables)) {
      console.warn('Invalid dependency data: missing tables array');
      return null;
    }

    const method =
      raw.detectionMethod === 'python-ast' ? 'python-ast' : 'sql-parser';

    return normalizeAndFilterDeps(raw.tables, method);
  } catch (error) {
    console.error('Failed to parse dependency JSON:', error);
    return null;
  }
}

/**
 * Check if a table reference looks like a real table vs an alias/temp.
 *
 * @param tableRef - Table reference in database.table format
 * @returns true if likely a real table, false if likely temp/alias
 */
export function isLikelyRealTable(tableRef: string): boolean {
  // Remove spark_catalog prefix
  const clean = tableRef.replace(/^spark_catalog\./, '');

  // Check for temp/default databases
  if (clean.startsWith('default.') || clean.startsWith('global_temp.')) {
    return false;
  }

  // Parse and check lengths
  const parts = clean.split('.');
  if (parts.length !== 2) {
    return false;
  }

  const [db, tbl] = parts;

  // Short names are likely aliases
  if (db.length <= 1 || tbl.length <= 2) {
    return false;
  }

  return true;
}

/**
 * Merge new actual dependencies with existing ones.
 * Used when accumulating dependencies at the table level.
 *
 * @param existing - Existing actual dependencies on the table
 * @param newDeps - Newly detected dependencies
 * @returns Merged dependencies (union, no duplicates)
 */
export function mergeActualDeps(
  existing: ActualDependency[],
  newDeps: ActualDependency[]
): ActualDependency[] {
  const seen = new Set<string>();
  const result: ActualDependency[] = [];

  // Add existing first
  for (const dep of existing) {
    const key = `${dep.database}.${dep.table}`;
    if (!seen.has(key)) {
      seen.add(key);
      result.push(dep);
    }
  }

  // Add new ones that don't exist
  for (const dep of newDeps) {
    const key = `${dep.database}.${dep.table}`;
    if (!seen.has(key)) {
      seen.add(key);
      result.push(dep);
    }
  }

  return result;
}

/**
 * Remove stale dependencies that are no longer detected.
 * Only removes if:
 * 1. The dependency is not in the new detection
 * 2. The ETL version has changed (indicating code was updated)
 *
 * @param existing - Existing actual dependencies
 * @param newDeps - Newly detected dependencies
 * @param versionChanged - Whether the ETL version changed
 * @returns Updated dependencies (removes stale ones only if version changed)
 */
export function removeStaleActualDeps(
  existing: ActualDependency[],
  newDeps: ActualDependency[],
  versionChanged: boolean
): ActualDependency[] {
  if (!versionChanged) {
    // If version hasn't changed, keep all existing deps
    // New deps will be merged separately
    return existing;
  }

  // Version changed - only keep deps that are still detected
  const newDepKeys = new Set(
    newDeps.map((d) => `${d.database}.${d.table}`)
  );

  return existing.filter((dep) => {
    const key = `${dep.database}.${dep.table}`;
    return newDepKeys.has(key);
  });
}

/**
 * Python code template for extracting actual dependencies from a SQL query
 * using Spark's analyzed logical plan.
 *
 * Uses _dvc_extract_tables_from_plan() but adapted to use
 * _explain_string(extended=True) instead of _jdf.queryExecution().analyzed()
 * because the local Spark sidecar uses Spark Connect which doesn't support
 * JVM interop (_jdf).
 *
 * The Spark Connect plan output differs slightly:
 * - Uses `local.database.table` catalog prefix instead of `spark_catalog.`
 * - SubqueryAlias produces `local.database.table` (3-part with catalog)
 * - Same UnresolvedRelation patterns
 */
const DEP_EXTRACTION_PYTHON = `
import re
import json

# Execute the user SQL to get an analyzed plan
df = spark.sql("""{{ESCAPED_SQL}}""")

# Extract dependencies from the Spark Connect analyzed plan.
# Note: all logic is inline (not in nested functions) because the sidecar's
# exec() uses separate globals/locals dicts, which breaks nested function calls.
try:
    plan_string = df._explain_string(extended=True)

    # Get temp views to filter out
    temp_views = set()
    try:
        for _t in spark.catalog.listTables():
            if _t.tableType == 'TEMPORARY':
                temp_views.add(_t.name.lower())
        try:
            for _t in spark.catalog.listTables('global_temp'):
                temp_views.add(_t.name.lower())
                temp_views.add(f'global_temp.{_t.name.lower()}')
        except:
            pass
    except:
        pass

    # Extract only the Analyzed Logical Plan section
    analyzed_section = ''
    _in_analyzed = False
    for _line in plan_string.splitlines():
        if '== Analyzed Logical Plan ==' in _line:
            _in_analyzed = True
            continue
        elif _in_analyzed and _line.startswith('== '):
            break
        elif _in_analyzed:
            analyzed_section += _line + '\\n'
    if not analyzed_section:
        analyzed_section = plan_string

    # Extract CTE names to filter
    cte_names = set()
    for _m in re.finditer(r'CTERelationRef\\s+(\\d+),\\s+(\\w+)', analyzed_section):
        cte_names.add(_m.group(2).lower())
    for _m in re.finditer(r'WithCTE.*?CTERelationDef\\s+(\\d+),\\s+(\\w+)', analyzed_section, re.DOTALL):
        cte_names.add(_m.group(2).lower())

    _tables = set()

    # Pattern 1: SubqueryAlias (Spark Connect uses catalog.database.table)
    # e.g., SubqueryAlias local.default.api_test2
    for _m in re.finditer(r'SubqueryAlias\\s+([\\w\\-\\.]+)', analyzed_section):
        _ref = _m.group(1)
        _parts = _ref.split('.')
        if len(_parts) == 3:
            _db, _tbl = _parts[1], _parts[2]
        elif len(_parts) == 2:
            _db, _tbl = _parts[0], _parts[1]
        else:
            continue
        if _tbl.lower() in temp_views or _tbl.lower() in cte_names:
            continue
        if _db.lower() == 'global_temp':
            continue
        _tables.add(f'{_db}.{_tbl}')

    # Pattern 2: catalog.database.table references (spark_catalog or local)
    for _m in re.finditer(r'(?:spark_catalog|local)\\.([\\w\\.]+)', analyzed_section):
        _full = _m.group(1)
        if '.' in _full:
            _parts = _full.split('.')
            if len(_parts) >= 2:
                _dbt = f'{_parts[0]}.{_parts[1]}'
                if _parts[-1].lower() not in temp_views and _parts[-1].lower() not in cte_names:
                    _tables.add(_dbt)

    # Pattern 3: UnresolvedRelation [database, table]
    for _m in re.finditer(r'UnresolvedRelation\\s+\\[([\\w\\.\\-,\\s]+)\\]', analyzed_section):
        _parts = [p.strip() for p in _m.group(1).split(',')]
        if len(_parts) >= 2:
            _db, _tbl = _parts[0], _parts[1]
            if _tbl.lower() not in temp_views and _tbl.lower() not in cte_names:
                if _db.lower() != 'global_temp':
                    _tables.add(f'{_db}.{_tbl}')

    _dep_tables = sorted(list(_tables))
    _dep_confidence = 1.0
    _dep_error = ''
except Exception as _e:
    _dep_tables = []
    _dep_confidence = 0.0
    _dep_error = str(_e)

_result = spark.createDataFrame([{
    "tables_json": json.dumps(_dep_tables),
    "confidence": float(_dep_confidence),
    "error": _dep_error
}])
`.trim();

/**
 * Build PySpark code that extracts actual dependencies from a SQL query
 * using Spark's analyzed logical plan.
 *
 * The generated code runs the SQL via spark.sql() to get the analyzed plan,
 * then parses it using _dvc_extract_tables_from_plan().
 *
 * @param sql - The SQL query to analyze
 * @returns Python code string to pass to executePython()
 */
export function buildDepExtractionCode(sql: string): string {
  // Escape for Python triple-quoted string: replace \ with \\, """ with \"\"\"
  const escaped = sql
    .replace(/\\/g, '\\\\')
    .replace(/"""/g, '\\"\\"\\"');
  return DEP_EXTRACTION_PYTHON.replace('{{ESCAPED_SQL}}', escaped);
}

/**
 * Parse the DataFrame result from Spark dependency extraction.
 *
 * Unlike normalizeAndFilterDeps() (which filters `default.*` for cloud S3-based
 * extraction), this function trusts the Python-side filtering that already
 * excluded temp views and CTEs. It only removes `spark_catalog.` prefixes
 * and deduplicates. This is important for local mode where `default` is a
 * legitimate database name.
 *
 * @param rows - Rows returned from executePython()
 * @param detectionMethod - How the dependencies were detected
 * @returns Actual dependencies parsed from Spark plan extraction
 */
export function parseDepExtractionResult(
  rows: Record<string, unknown>[],
  detectionMethod: 'sql-parser' | 'python-ast' = 'sql-parser'
): ActualDependency[] {
  if (!rows || rows.length === 0) {
    return [];
  }

  const row = rows[0];
  const tablesJson = row['tables_json'] as string | undefined;
  const error = row['error'] as string | undefined;

  if (error) {
    console.warn('Spark dependency extraction error:', error);
  }

  if (!tablesJson) {
    return [];
  }

  try {
    const tables = JSON.parse(tablesJson) as string[];
    const seen = new Set<string>();
    const result: ActualDependency[] = [];

    for (const table of tables) {
      // Remove spark_catalog prefix
      const clean = table.replace(/^spark_catalog\./, '');
      const parts = clean.split('.');
      if (parts.length !== 2) continue;

      const [db, tbl] = parts;
      const key = `${db}.${tbl}`;
      if (seen.has(key)) continue;
      seen.add(key);

      result.push({
        database: db,
        table: tbl,
        confidence: 1.0,
        detectionMethod,
      });
    }

    return result;
  } catch {
    console.warn('Failed to parse dependency extraction result');
    return [];
  }
}
