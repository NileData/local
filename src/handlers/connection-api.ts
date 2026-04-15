import { randomUUID } from 'crypto';
import type { RequestHandler } from 'express';
import type { SparkEngine } from '../engines/spark-engine.js';
import type { SQLiteService } from '../services/sqlite-service.js';
import type {
  CreateConnectionRequest,
  ConnectionType,
  DVCConnectionRecord,
  TestConnectionRequest,
  TestConnectionResponse,
  ListConnectionTablesResponse,
  ConnectionTableInfo,
  DetectConnectionSchemaResponse,
  ConnectionColumnInfo,
  ListConnectionsResponse,
  GetConnectionResponse,
  CreateConnectionResponse,
  DeleteConnectionResponse,
  ListConnectionTablesRequestBody,
  DetectConnectionSchemaRequestBody,
  QueryExternalConnectionRequest,
  QueryExternalConnectionResponse,
} from '../types/types.js';

// ── Internal types for SQLite row → config mapping ──────────────────

/** Shape of the config JSON stored in SQLite for each connection. */
interface StoredConnectionConfig {
  host?: string;
  port?: number;
  database: string;
  username: string;
  password: string;
  sslMode?: string;
  account?: string;
  warehouse?: string;
  role?: string;
  schema?: string;
  provider?: string;
  description?: string;
}

/** Row shape returned by SQLiteService connection methods. */
interface SQLiteConnectionRow {
  connectorId: string;
  name: string;
  type: string;
  configJson: string;
  createdAt: string;
  updatedAt?: string;
}

// ── JDBC helpers ────────────────────────────────────────────────────

interface JdbcOptions {
  url: string;
  driver: string;
  properties: Record<string, string>;
}

function buildJdbcOptions(connectionType: string, config: StoredConnectionConfig): JdbcOptions {
  switch (connectionType) {
    case 'postgres': {
      const host = config.host ?? 'localhost';
      const port = config.port ?? 5432;
      const sslParam = config.sslMode ? `&sslmode=${config.sslMode}` : '';
      return {
        url: `jdbc:postgresql://${host}:${port}/${config.database}?stringtype=unspecified${sslParam}`,
        driver: 'org.postgresql.Driver',
        properties: {
          user: config.username,
          password: config.password,
        },
      };
    }
    case 'snowflake': {
      const account = config.account ?? '';
      const params = new URLSearchParams();
      params.set('db', config.database);
      if (config.schema) params.set('schema', config.schema);
      if (config.warehouse) params.set('warehouse', config.warehouse);
      if (config.role) params.set('role', config.role);
      return {
        url: `jdbc:snowflake://${account}.snowflakecomputing.com/?${params.toString()}`,
        driver: 'net.snowflake.client.jdbc.SnowflakeDriver',
        properties: {
          user: config.username,
          password: config.password,
        },
      };
    }
    case 'mysql': {
      const host = config.host ?? 'localhost';
      const port = config.port ?? 3306;
      return {
        url: `jdbc:mysql://${host}:${port}/${config.database}`,
        driver: 'com.mysql.cj.jdbc.Driver',
        properties: {
          user: config.username,
          password: config.password,
        },
      };
    }
    default:
      throw new Error(`Unsupported connection type: ${connectionType}`);
  }
}

/** Generate a unique temp view name using timestamp + random suffix. */
function tempViewName(prefix: string): string {
  return `${prefix}_${Date.now()}_${randomUUID().slice(0, 8).replace(/-/g, '')}`;
}

// ── Mapping helpers ─────────────────────────────────────────────────

function parseConfig(configJson: string): StoredConnectionConfig {
  return JSON.parse(configJson) as StoredConnectionConfig;
}

/** Convert a SQLite row to a DVCConnectionRecord, masking the password. */
function rowToRecord(row: SQLiteConnectionRow, maskPassword: boolean): DVCConnectionRecord {
  const config = parseConfig(row.configJson);
  return {
    connectionId: row.connectorId,
    name: row.name,
    connectionType: row.type as ConnectionType,
    host: config.host,
    port: config.port,
    database: config.database,
    username: maskPassword ? config.username : config.username,
    sslMode: config.sslMode,
    account: config.account,
    warehouse: config.warehouse,
    role: config.role,
    schema: config.schema,
    provider: config.provider as DVCConnectionRecord['provider'],
    description: config.description,
    // Local mode has no Secrets Manager; use placeholder
    secretArn: 'local',
    createdAt: row.createdAt,
    updatedAt: row.updatedAt ?? row.createdAt,
    // Mask the password in returned records
    ...(maskPassword ? { password: '***' } : {}),
  };
}

/** Build a StoredConnectionConfig from a CreateConnectionRequest body. */
function bodyToConfig(body: CreateConnectionRequest): StoredConnectionConfig {
  return {
    host: body.host,
    port: body.port,
    database: body.database,
    username: body.username,
    password: body.password,
    sslMode: body.sslMode,
    account: body.account,
    warehouse: body.warehouse,
    role: body.role,
    schema: body.schema,
    provider: body.provider,
    description: body.description,
  };
}

/** Resolve connection config: either from body fields directly, or from a saved connection in SQLite. */
function resolveConnectionConfig(
  body: TestConnectionRequest,
  sqliteService: SQLiteService,
  connectionIdOverride?: string,
): { connectionType: string; config: StoredConnectionConfig } {
  const connId = connectionIdOverride ?? body.connectionId;
  if (connId) {
    const row = sqliteService.getConnection(connId) as SQLiteConnectionRow | undefined;
    if (!row) {
      throw new Error(`Connection not found: ${connId}`);
    }
    const config = parseConfig(row.configJson);
    return { connectionType: row.type, config };
  }

  // Direct connection details from body
  if (!body.connectionType) {
    throw new Error('connectionType is required when connectionId is not provided');
  }
  if (!body.database) {
    throw new Error('database is required');
  }
  if (!body.username) {
    throw new Error('username is required');
  }
  if (!body.password) {
    throw new Error('password is required');
  }

  return {
    connectionType: body.connectionType,
    config: {
      host: body.host,
      port: body.port,
      database: body.database,
      username: body.username,
      password: body.password,
      sslMode: body.sslMode,
      account: body.account,
      warehouse: body.warehouse,
      role: body.role,
      schema: body.schema,
    },
  };
}

// ── Spark JDBC execution helpers ────────────────────────────────────

/**
 * Execute a SQL query against an external database via Spark JDBC temp view.
 * Creates a temp view, runs the query, drops the view, and returns rows.
 */
async function executeJdbcQuery(
  sparkEngine: SparkEngine,
  connectionType: string,
  config: StoredConnectionConfig,
  dbtable: string,
): Promise<{ columns: Array<{ name: string; type: string }>; rows: Record<string, unknown>[] }> {
  const jdbc = buildJdbcOptions(connectionType, config);
  const viewName = tempViewName('_conn');

  // Escape single quotes in JDBC properties for SQL safety
  const escape = (s: string): string => s.replace(/'/g, "\\'");

  const createViewSQL = [
    `CREATE TEMPORARY VIEW ${viewName}`,
    `USING jdbc`,
    `OPTIONS (`,
    `  url '${escape(jdbc.url)}',`,
    `  dbtable '${escape(dbtable)}',`,
    `  user '${escape(jdbc.properties['user'])}',`,
    `  password '${escape(jdbc.properties['password'])}',`,
    `  driver '${escape(jdbc.driver)}'`,
    `)`,
  ].join('\n');

  try {
    await sparkEngine.executeDDL(createViewSQL);
    const result = await sparkEngine.executeSQL(`SELECT * FROM ${viewName}`);
    return { columns: result.columns, rows: result.rows };
  } finally {
    try {
      await sparkEngine.executeDDL(`DROP VIEW IF EXISTS ${viewName}`);
    } catch {
      // Non-critical cleanup failure
    }
  }
}

// ── Read-only query validation ───────────────────────────────────────

function isReadOnlyQuery(sql: string): boolean {
  const trimmed = sql.trim().replace(/^\/\*.*?\*\//s, '').trim();
  const firstWord = trimmed.split(/\s+/)[0].toUpperCase();
  return ['SELECT', 'WITH', 'EXPLAIN', 'SHOW', 'DESCRIBE'].includes(firstWord);
}

// ── Handler factory ─────────────────────────────────────────────────

export function createConnectionHandlers(
  sparkEngine: SparkEngine,
  sqliteService: SQLiteService,
): Record<string, RequestHandler> {

  // ── CreateConnectionAPI ─────────────────────────────────────────
  const create: RequestHandler = async (req, res) => {
    try {
      const body = req.body as CreateConnectionRequest;
      if (!body.name || !body.connectionType || !body.database || !body.username || !body.password) {
        res.status(400).json({
          success: false,
          error: 'Missing required fields: name, connectionType, database, username, password',
        } satisfies CreateConnectionResponse);
        return;
      }

      const connectionId = randomUUID();
      const config = bodyToConfig(body);

      sqliteService.insertConnection(
        connectionId,
        body.name,
        body.connectionType,
        JSON.stringify(config),
      );

      const row = sqliteService.getConnection(connectionId) as SQLiteConnectionRow | undefined;
      const connection = row ? rowToRecord(row, true) : undefined;

      const response: CreateConnectionResponse = {
        success: true,
        connectionId,
        connection,
      };
      res.json(response);
    } catch (err) {
      res.status(500).json({
        success: false,
        error: err instanceof Error ? err.message : String(err),
      } satisfies CreateConnectionResponse);
    }
  };

  // ── ListConnections ─────────────────────────────────────────────
  const list: RequestHandler = (_req, res) => {
    try {
      const rows = sqliteService.listConnections() as SQLiteConnectionRow[];
      const connections: DVCConnectionRecord[] = rows.map(row => rowToRecord(row, true));

      const response: ListConnectionsResponse = { success: true, connections };
      res.json(response);
    } catch (err) {
      res.status(500).json({
        connections: [],
        error: err instanceof Error ? err.message : String(err),
      });
    }
  };

  // ── GetConnection ───────────────────────────────────────────────
  const get: RequestHandler = (req, res) => {
    try {
      const { connectionId } = req.params;
      const row = sqliteService.getConnection(connectionId) as SQLiteConnectionRow | undefined;
      if (!row) {
        res.status(404).json({ error: `Connection not found: ${connectionId}` });
        return;
      }

      const response: GetConnectionResponse = {
        success: true,
        connection: rowToRecord(row, true),
      };
      res.json(response);
    } catch (err) {
      res.status(500).json({ error: err instanceof Error ? err.message : String(err) });
    }
  };

  // ── DeleteConnection ────────────────────────────────────────────
  const deleteConn: RequestHandler = (req, res) => {
    try {
      const { connectionId } = req.params;
      const row = sqliteService.getConnection(connectionId) as SQLiteConnectionRow | undefined;
      if (!row) {
        res.status(404).json({ success: false, error: `Connection not found: ${connectionId}` });
        return;
      }

      sqliteService.deleteConnection(connectionId);

      const response: DeleteConnectionResponse = { success: true };
      res.json(response);
    } catch (err) {
      res.status(500).json({
        success: false,
        error: err instanceof Error ? err.message : String(err),
      } satisfies DeleteConnectionResponse);
    }
  };

  // ── TestConnectionAPI ───────────────────────────────────────────
  const testConnection: RequestHandler = async (req, res) => {
    const startMs = Date.now();
    try {
      const body = req.body as TestConnectionRequest;
      const { connectionType, config } = resolveConnectionConfig(body, sqliteService);
      const jdbc = buildJdbcOptions(connectionType, config);
      const viewName = tempViewName('_test');

      const escape = (s: string): string => s.replace(/'/g, "\\'");
      const createSQL = [
        `CREATE TEMPORARY VIEW ${viewName}`,
        `USING jdbc`,
        `OPTIONS (`,
        `  url '${escape(jdbc.url)}',`,
        `  dbtable '(SELECT 1 AS ok) AS t',`,
        `  user '${escape(jdbc.properties['user'])}',`,
        `  password '${escape(jdbc.properties['password'])}',`,
        `  driver '${escape(jdbc.driver)}'`,
        `)`,
      ].join('\n');

      await sparkEngine.executeDDL(createSQL);
      await sparkEngine.executeSQL(`SELECT * FROM ${viewName}`);

      try {
        await sparkEngine.executeDDL(`DROP VIEW IF EXISTS ${viewName}`);
      } catch {
        // Non-critical cleanup
      }

      const latencyMs = Date.now() - startMs;
      const response: TestConnectionResponse = {
        success: true,
        latencyMs,
      };
      res.json(response);
    } catch (err) {
      const latencyMs = Date.now() - startMs;
      const response: TestConnectionResponse = {
        success: false,
        latencyMs,
        error: err instanceof Error ? err.message : String(err),
      };
      res.json(response);
    }
  };

  // ── TestSavedConnection ─────────────────────────────────────────
  const testSaved: RequestHandler = async (req, res) => {
    const startMs = Date.now();
    try {
      const { connectionId } = req.params;
      const { connectionType, config } = resolveConnectionConfig({}, sqliteService, connectionId);
      const jdbc = buildJdbcOptions(connectionType, config);
      const viewName = tempViewName('_test');

      const escape = (s: string): string => s.replace(/'/g, "\\'");
      const createSQL = [
        `CREATE TEMPORARY VIEW ${viewName}`,
        `USING jdbc`,
        `OPTIONS (`,
        `  url '${escape(jdbc.url)}',`,
        `  dbtable '(SELECT 1 AS ok) AS t',`,
        `  user '${escape(jdbc.properties['user'])}',`,
        `  password '${escape(jdbc.properties['password'])}',`,
        `  driver '${escape(jdbc.driver)}'`,
        `)`,
      ].join('\n');

      await sparkEngine.executeDDL(createSQL);
      await sparkEngine.executeSQL(`SELECT * FROM ${viewName}`);

      try {
        await sparkEngine.executeDDL(`DROP VIEW IF EXISTS ${viewName}`);
      } catch {
        // Non-critical cleanup
      }

      // Update last tested timestamp
      sqliteService.updateConnection(connectionId, {
        configJson: undefined, // no config change
      });

      const latencyMs = Date.now() - startMs;
      const response: TestConnectionResponse = {
        success: true,
        latencyMs,
      };
      res.json(response);
    } catch (err) {
      const latencyMs = Date.now() - startMs;
      const response: TestConnectionResponse = {
        success: false,
        latencyMs,
        error: err instanceof Error ? err.message : String(err),
      };
      res.json(response);
    }
  };

  // ── ListConnectionTables ────────────────────────────────────────
  const listTables: RequestHandler = async (req, res) => {
    try {
      const { connectionId } = req.params;
      const body = (req.body ?? {}) as ListConnectionTablesRequestBody;
      const { connectionType, config } = resolveConnectionConfig({}, sqliteService, connectionId);

      // Determine the schema to query
      const schemaName = body.schema ?? config.schema ?? (connectionType === 'snowflake' ? 'PUBLIC' : 'public');

      let dbtable: string;
      if (connectionType === 'snowflake') {
        dbtable = `(SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${schemaName}') AS t`;
      } else {
        // postgres / mysql
        dbtable = `(SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = '${schemaName}') AS t`;
      }

      const result = await executeJdbcQuery(sparkEngine, connectionType, config, dbtable);

      const tables: ConnectionTableInfo[] = result.rows.map(row => ({
        schema: String(row['table_schema'] ?? row['TABLE_SCHEMA'] ?? schemaName),
        tableName: String(row['table_name'] ?? row['TABLE_NAME'] ?? ''),
        tableType: String(row['table_type'] ?? row['TABLE_TYPE'] ?? 'BASE TABLE'),
      }));

      const response: ListConnectionTablesResponse = {
        success: true,
        tables,
      };
      res.json(response);
    } catch (err) {
      const response: ListConnectionTablesResponse = {
        success: false,
        error: err instanceof Error ? err.message : String(err),
      };
      res.json(response);
    }
  };

  // ── DetectConnectionSchema ──────────────────────────────────────
  const detectSchema: RequestHandler = async (req, res) => {
    try {
      const { connectionId } = req.params;
      const body = req.body as DetectConnectionSchemaRequestBody;
      if (!body.schema || !body.tableName) {
        res.status(400).json({
          success: false,
          error: 'schema and tableName are required',
        } satisfies DetectConnectionSchemaResponse);
        return;
      }

      const { connectionType, config } = resolveConnectionConfig({}, sqliteService, connectionId);
      const sampleSize = body.sampleSize ?? 100;

      // 1. Fetch column metadata from information_schema
      let columnDbtable: string;
      if (connectionType === 'snowflake') {
        columnDbtable = [
          `(SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION`,
          `FROM INFORMATION_SCHEMA.COLUMNS`,
          `WHERE TABLE_SCHEMA = '${body.schema}' AND TABLE_NAME = '${body.tableName}'`,
          `ORDER BY ORDINAL_POSITION) AS t`,
        ].join(' ');
      } else {
        columnDbtable = [
          `(SELECT column_name, data_type, is_nullable, column_default, ordinal_position`,
          `FROM information_schema.columns`,
          `WHERE table_schema = '${body.schema}' AND table_name = '${body.tableName}'`,
          `ORDER BY ordinal_position) AS t`,
        ].join(' ');
      }

      const colResult = await executeJdbcQuery(sparkEngine, connectionType, config, columnDbtable);

      const columns: ConnectionColumnInfo[] = colResult.rows.map(row => {
        const sourceType = String(row['data_type'] ?? row['DATA_TYPE'] ?? 'string');
        return {
          name: String(row['column_name'] ?? row['COLUMN_NAME'] ?? ''),
          sourceType,
          dvcType: mapSourceTypeToIceberg(sourceType, connectionType),
          nullable: String(row['is_nullable'] ?? row['IS_NULLABLE'] ?? 'YES').toUpperCase() === 'YES',
          defaultValue: row['column_default'] != null ? String(row['column_default'] ?? row['COLUMN_DEFAULT']) : undefined,
          isPrimaryKey: false, // Would require additional query; left as false for now
          ordinalPosition: Number(row['ordinal_position'] ?? row['ORDINAL_POSITION'] ?? 0),
        };
      });

      // 2. Fetch sample rows
      const qualifiedTable = `${body.schema}.${body.tableName}`;
      const sampleDbtable = `(SELECT * FROM ${qualifiedTable} LIMIT ${sampleSize}) AS t`;

      let sampleRows: Record<string, unknown>[] = [];
      try {
        const sampleResult = await executeJdbcQuery(sparkEngine, connectionType, config, sampleDbtable);
        sampleRows = sampleResult.rows;
      } catch {
        // Sample fetch is non-critical; return columns without samples
      }

      // 3. Determine partition column candidates (date, timestamp, numeric types)
      const partitionCandidateTypes = new Set([
        'date', 'timestamp', 'timestamptz', 'timestamp without time zone',
        'timestamp with time zone', 'integer', 'int', 'bigint', 'int4', 'int8',
        'number', 'float', 'double', 'decimal', 'numeric',
        'DATE', 'TIMESTAMP', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ',
        'NUMBER', 'FLOAT', 'INTEGER', 'BIGINT',
      ]);
      const partitionColumnCandidates = columns
        .filter(c => partitionCandidateTypes.has(c.sourceType))
        .map(c => c.name);

      const response: DetectConnectionSchemaResponse = {
        success: true,
        columns,
        sampleRows,
        rowCount: String(sampleRows.length),
        partitionColumnCandidates,
      };
      res.json(response);
    } catch (err) {
      const response: DetectConnectionSchemaResponse = {
        success: false,
        error: err instanceof Error ? err.message : String(err),
      };
      res.json(response);
    }
  };

  // ── QueryExternalConnection ──────────────────────────────────────
  const queryExternal: RequestHandler = async (req, res) => {
    const startTime = Date.now();
    try {
      const { connectionId } = req.params;
      const body = req.body as QueryExternalConnectionRequest;

      if (!body.query) {
        res.status(400).json({ success: false, error: 'query is required' } satisfies QueryExternalConnectionResponse);
        return;
      }

      if (!isReadOnlyQuery(body.query)) {
        res.status(400).json({
          success: false,
          error: 'Only read-only queries are allowed (SELECT, WITH, EXPLAIN, SHOW, DESCRIBE)',
        } satisfies QueryExternalConnectionResponse);
        return;
      }

      const { connectionType, config } = resolveConnectionConfig({}, sqliteService, connectionId);
      const maxRows = Math.min(body.maxRows ?? 100, 1000);

      // Use a subquery with LIMIT to cap rows via JDBC temp view
      const dbtable = `(${body.query} LIMIT ${maxRows + 1}) AS _q`;
      const result = await executeJdbcQuery(sparkEngine, connectionType, config, dbtable);

      const truncated = result.rows.length > maxRows;
      const rows = result.rows.slice(0, maxRows);
      const columns = result.columns.map(c => c.name);

      const response: QueryExternalConnectionResponse = {
        success: true,
        columns,
        rows: rows.map(row => columns.map(col => row[col] as unknown)),
        rowCount: rows.length,
        truncated,
        executionTimeMs: String(Date.now() - startTime),
      };
      res.json(response);
    } catch (err) {
      const response: QueryExternalConnectionResponse = {
        success: false,
        error: err instanceof Error ? err.message : String(err),
        executionTimeMs: String(Date.now() - startTime),
      };
      res.json(response);
    }
  };

  return {
    'CreateConnectionAPI': create,
    'ListConnections': list,
    'GetConnection': get,
    'DeleteConnection': deleteConn,
    'TestConnectionAPI': testConnection,
    'TestSavedConnection': testSaved,
    'ListConnectionTables': listTables,
    'DetectConnectionSchema': detectSchema,
    'QueryExternalConnection': queryExternal,
  };
}

// ── Type mapping helper ─────────────────────────────────────────────

/**
 * Map source database types to Iceberg/DVC types.
 * This is a best-effort mapping for schema detection.
 */
function mapSourceTypeToIceberg(sourceType: string, connectionType: string): string {
  const normalized = sourceType.toLowerCase().trim();

  // PostgreSQL type mappings
  if (connectionType === 'postgres' || connectionType === 'mysql') {
    const pgMap: Record<string, string> = {
      'integer': 'int',
      'int': 'int',
      'int4': 'int',
      'smallint': 'int',
      'int2': 'int',
      'bigint': 'long',
      'int8': 'long',
      'serial': 'int',
      'bigserial': 'long',
      'real': 'float',
      'float4': 'float',
      'double precision': 'double',
      'float8': 'double',
      'numeric': 'decimal',
      'decimal': 'decimal',
      'money': 'decimal',
      'boolean': 'boolean',
      'bool': 'boolean',
      'text': 'string',
      'character varying': 'string',
      'varchar': 'string',
      'character': 'string',
      'char': 'string',
      'name': 'string',
      'uuid': 'string',
      'json': 'string',
      'jsonb': 'string',
      'xml': 'string',
      'date': 'date',
      'timestamp without time zone': 'timestamp',
      'timestamp with time zone': 'timestamptz',
      'timestamp': 'timestamp',
      'timestamptz': 'timestamptz',
      'time': 'string',
      'time without time zone': 'string',
      'time with time zone': 'string',
      'interval': 'string',
      'bytea': 'binary',
      'bit': 'string',
      'bit varying': 'string',
      'inet': 'string',
      'cidr': 'string',
      'macaddr': 'string',
      'tinyint': 'int',
      'mediumint': 'int',
      'float': 'float',
      'double': 'double',
      'datetime': 'timestamp',
      'blob': 'binary',
      'longtext': 'string',
      'mediumtext': 'string',
      'tinytext': 'string',
      'enum': 'string',
      'set': 'string',
    };
    return pgMap[normalized] ?? 'string';
  }

  // Snowflake type mappings
  if (connectionType === 'snowflake') {
    const sfMap: Record<string, string> = {
      'number': 'decimal',
      'decimal': 'decimal',
      'numeric': 'decimal',
      'int': 'int',
      'integer': 'int',
      'bigint': 'long',
      'smallint': 'int',
      'tinyint': 'int',
      'byteint': 'int',
      'float': 'double',
      'float4': 'float',
      'float8': 'double',
      'double': 'double',
      'double precision': 'double',
      'real': 'float',
      'varchar': 'string',
      'char': 'string',
      'character': 'string',
      'string': 'string',
      'text': 'string',
      'binary': 'binary',
      'varbinary': 'binary',
      'boolean': 'boolean',
      'date': 'date',
      'datetime': 'timestamp',
      'time': 'string',
      'timestamp': 'timestamp',
      'timestamp_ltz': 'timestamptz',
      'timestamp_ntz': 'timestamp',
      'timestamp_tz': 'timestamptz',
      'variant': 'string',
      'object': 'string',
      'array': 'string',
    };
    return sfMap[normalized] ?? 'string';
  }

  return 'string';
}
