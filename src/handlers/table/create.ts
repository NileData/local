import { copyFileSync, mkdirSync } from 'fs';
import { join, basename } from 'path';
import type { RequestHandler } from 'express';
import type { SparkEngine } from '../../engines/spark-engine.js';
import type { SQLiteService } from '../../services/sqlite-service.js';
import type { LocalConfig } from '../../config/environment.js';
import type { GetTableResponse, Column } from '../../types/types.js';
import { validateCreateTableFields, tableAlreadyExistsError, httpStatusForError } from '../../lib/shared-api/index.js';

export function createTableCreateHandler(config: LocalConfig, sparkEngine: SparkEngine, sqliteService: SQLiteService): RequestHandler {
  return async (req, res) => {
    const body = req.body as Record<string, unknown> | undefined;
    const database = body?.database as string | undefined;
    const tableName = body?.tableName as string | undefined;
    const sourcePath = body?.sourcePath as string | undefined;
    const sourceSql = body?.sourceSql as string | undefined;
    const updateStrategy = body?.updateStrategy as string | undefined;

    const fieldError = validateCreateTableFields({ database, tableName });
    if (fieldError) {
      res.status(httpStatusForError(fieldError)).json({ error: fieldError.errorCode, message: fieldError.message });
      return;
    }

    const allowedDb = config.local.teamName;
    if (database !== allowedDb) {
      res.status(403).json({
        error: 'DATABASE_NAME_RESTRICTED',
        message: `Tables can only be created in the team database "${allowedDb}". Cannot use database "${database}".`,
      });
      return;
    }

    // After validation, database and tableName are guaranteed present
    const db = database!;
    const tbl = tableName!;

    try {
      // Check if table already exists
      const existingTable = sqliteService.getTable(db, tbl);
      let tableExistsInSpark = false;
      try {
        await sparkEngine.executeSQL(`SELECT 1 FROM ${db}.${tbl} LIMIT 0`);
        tableExistsInSpark = true;
      } catch {
        // Table doesn't exist in Spark
      }

      if (existingTable || tableExistsInSpark) {
        if (updateStrategy === 'replace') {
          // Drop and recreate to apply new schema
          try { await sparkEngine.executeDDL(`DROP TABLE IF EXISTS ${db}.${tbl}`); } catch { /* may not exist in Spark */ }
          sqliteService.deleteTable(db, tbl);
        } else {
          const existsError = tableAlreadyExistsError(db, tbl);
          res.status(httpStatusForError(existsError)).json({ error: existsError.errorCode, message: existsError.message });
          return;
        }
      }

      // Ensure namespace exists in Iceberg catalog (may fail if name collides with catalog)
      try {
        await sparkEngine.executeDDL(`CREATE NAMESPACE IF NOT EXISTS ${db}`);
      } catch {
        // Namespace likely already exists
      }

      if (sourceSql) {
        const codeLanguage = (body?.code as Record<string, unknown> | undefined)?.language as string | undefined;
        const tempView = `_sat_${Date.now()}`;
        const satCode = buildSATCode({ userCode: sourceSql, language: codeLanguage || 'sql', tempView });

        if (satCode.mode === 'python') {
          await sparkEngine.executePython(satCode.code);
          await sparkEngine.executeDDL(
            `CREATE TABLE ${db}.${tbl} USING ICEBERG AS SELECT * FROM ${tempView}`
          );
          try { await sparkEngine.executeDDL(`DROP VIEW IF EXISTS ${tempView}`); } catch { /* non-critical */ }
        } else {
          // SQL CTAS
          await sparkEngine.executeDDL(
            `CREATE TABLE ${db}.${tbl} USING ICEBERG AS ${satCode.code}`
          );
        }
      } else if (sourcePath) {
        // CTAS: Read the source file via Spark and create Iceberg table
        const containerPath = toContainerPath(sourcePath, sparkEngine);
        const ext = sourcePath.toLowerCase().split('.').pop() ?? '';
        const sparkFormat = ext === 'parquet' ? 'parquet' :
                            ext === 'json'    ? 'json' :
                                                'csv';

        // Create Iceberg table directly from file using Spark CTAS
        if (sparkFormat === 'csv') {
          const tempView = `_create_${Date.now()}`;
          await sparkEngine.executeDDL(
            `CREATE TEMPORARY VIEW ${tempView} USING csv OPTIONS (path '${containerPath}', header 'true', inferSchema 'true')`
          );
          await sparkEngine.executeDDL(
            `CREATE TABLE ${db}.${tbl} USING ICEBERG AS SELECT * FROM ${tempView}`
          );
          try { await sparkEngine.executeDDL(`DROP VIEW IF EXISTS ${tempView}`); } catch { /* non-critical */ }
        } else {
          await sparkEngine.executeDDL(
            `CREATE TABLE ${db}.${tbl} USING ICEBERG AS SELECT * FROM ${sparkFormat}.\`${containerPath}\``
          );
        }
      } else {
        // Empty table creation -- check if columns are provided in the body
        const columns = body?.columns as Array<{ name: string; dataType: string }> | undefined;
        if (columns && columns.length > 0) {
          const sparkCols = columns.map(c => `${c.name} ${c.dataType}`).join(', ');
          await sparkEngine.executeDDL(
            `CREATE TABLE ${db}.${tbl} (${sparkCols}) USING ICEBERG`
          );
        } else {
          // Minimal empty table with a single placeholder column
          await sparkEngine.executeDDL(
            `CREATE TABLE ${db}.${tbl} (id BIGINT) USING ICEBERG`
          );
        }
      }

      // Get schema and row count from Spark for catalog
      let schemaColumns: Column[] = [];
      try {
        const descResult = await sparkEngine.executeSQL(`DESCRIBE TABLE ${db}.${tbl}`);
        schemaColumns = descResult.rows.map(row => ({
          name: String(row['col_name']),
          dataType: String(row['data_type']),
          nullable: true,
        }));
      } catch {
        // Non-critical -- proceed without schema
      }

      let rowCount: number | undefined;
      if (sourceSql || sourcePath) {
        try {
          const countResult = await sparkEngine.executeSQL(`SELECT COUNT(*) AS row_count FROM ${db}.${tbl}`);
          rowCount = Number(countResult.rows[0]?.['row_count'] ?? 0);
        } catch {
          // Non-critical
        }
      }

      // Build definition object to persist (code, dependencies, update strategy)
      const description = body?.description as string | undefined;
      const codeObj = body?.code as Record<string, unknown> | undefined;
      const definition: Record<string, unknown> = {
        schema: schemaColumns,
        description: description || '',
        updateStrategy: updateStrategy || 'replace',
      };
      if (sourceSql) {
        definition.code = {
          content: sourceSql,
          language: 'sql',
          dialect: 'spark-sql',
        };
      }
      if (codeObj) {
        definition.code = codeObj;
      }

      // Store dependencies if provided (from SAT handler)
      const depsObj = body?.dependencies as Record<string, unknown> | undefined;
      if (depsObj) {
        definition.dependencies = depsObj;
      }

      sqliteService.insertDatabase(db); // Ensure DB exists in catalog
      const now = new Date().toISOString();
      const tableId = sqliteService.insertTable(db, tbl, {
        description,
        schemaJson: JSON.stringify(schemaColumns),
        rowCount,
        definitionJson: JSON.stringify(definition),
      });
      sqliteService.insertEvent('table_created', `Created table ${db}.${tbl}`, { tableId });

      // Record query history for CTAS operations
      if (sourceSql) {
        sqliteService.insertQueryHistory(sourceSql, db, rowCount ?? 0);
      }

      const response: GetTableResponse = {
        partitionKey: db,
        sortKey: `${tbl}.v1.main`,
        database: db,
        tableName: tbl,
        fullTableName: `${db}.${tbl}`,
        tableType: 'MANAGED',
        version: { major: 1, minor: 0, build: 0 },
        branch: 'v1.main',
        definition: definition as GetTableResponse['definition'],
        createdOn: now,
        updatedOn: now,
      };
      res.status(201).json(response);
    } catch (err) {
      res.status(500).json({ error: 'INTERNAL_ERROR', message: err instanceof Error ? err.message : String(err) });
    }
  };
}

/**
 * Build PySpark or SQL code for Save As Table execution.
 * For Python: wraps user code with transform_data() invocation, DataFrame validation,
 * and temp view creation for subsequent CTAS.
 * For SQL: returns user code as-is for direct CTAS.
 */
function buildSATCode(params: {
  userCode: string;
  language: string;
  tempView: string;
}): { code: string; mode: 'python' | 'sql-ctas' } {
  const { userCode, language, tempView } = params;

  if (language === 'python' || language === 'pyspark') {
    return {
      code: `
# User function definition
${userCode}

# Execute user function
result_df = transform_data(spark)

# Validate result
if result_df is None or not hasattr(result_df, 'count'):
    raise ValueError('transform_data() must return a DataFrame')

result_df.createOrReplaceTempView("${tempView}")
_result = result_df
`.trim(),
      mode: 'python',
    };
  }

  return { code: userCode, mode: 'sql-ctas' };
}

/**
 * Convert a host file path to a container path.
 * The Spark container mounts ~/.nile/data-lake as /warehouse.
 * If the file is already inside the data-lake dir, map it to /warehouse/...
 * Otherwise, copy the file into the staging area first.
 */
function toContainerPath(hostPath: string, sparkEngine: SparkEngine): string {
  const warehouseDir = sparkEngine.getWarehouseDir().replace(/\\/g, '/');
  const normalizedHost = hostPath.replace(/\\/g, '/');

  if (normalizedHost.startsWith(warehouseDir)) {
    const relative = normalizedHost.slice(warehouseDir.length);
    return `/warehouse${relative}`;
  }

  // File is outside warehouse -- copy it into a staging area
  const stagingDir = join(sparkEngine.getWarehouseDir(), '_staging');
  mkdirSync(stagingDir, { recursive: true });
  const destFile = join(stagingDir, basename(hostPath));
  copyFileSync(hostPath, destFile);
  return `/warehouse/_staging/${basename(hostPath)}`;
}
