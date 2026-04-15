export const SYSTEM_IMPORT_SKILL = {
  name: "import",
  description: "Import external data sources into the platform.",
  content: `# Data Import and Migration

## Import Tools Quick Reference

| Tool | Source | Table Type | Key Params |
|------|--------|------------|------------|
| detect_s3_schema | S3 | preview | s3Location |
| import_from_s3 | S3 files | EXTERNAL | s3Location, format, crawlerSchedule |
| detect_glue_schema | Glue | preview | sourceDatabase, sourceTable |
| import_from_glue | Glue catalog | EXTERNAL | sourceDatabase, sourceTable |
| import_from_paste | Direct data | MANAGED | schema, rows |
| import_from_postgres | PostgreSQL | MANAGED | connectionId, sourceTable |
| list_connections | - | - | - |
| create_connection | - | - | host, port, database, username, password |
| test_connection | - | - | host, port, database, username, password |
| list_connection_tables | - | - | connectionId |
| detect_connection_schema | - | - | connectionId, schema, tableName |
| query_external_connection | PostgreSQL/Snowflake/JDBC | read-only query | connectionId, query, maxRows |
| get_workflow_status | - | - | executionId (set this to the full executionArn value when available) |

## Import Workflows

### S3 Import

1. Call \`detect_s3_schema\` first to preview schema, format, and partitions
2. Confirm table name, format, and partition columns
3. Call \`import_from_s3\`
4. Wait for the async workflow result; only use \`get_workflow_status\` when the tool response did not already include final workflow completion

### Glue Import

1. Call \`detect_glue_schema\` to preview schema
2. Confirm the destination table name
3. Call \`import_from_glue\`
4. Wait for the async workflow result; only use \`get_workflow_status\` when the tool response did not already include final workflow completion

### PostgreSQL Import

1. Call \`list_connections\` to check for an existing connection
2. If none exists, call \`create_connection\`
3. Call \`test_connection\` before importing
4. Call \`list_connection_tables\`
5. Call \`detect_connection_schema\`
6. Call \`import_from_postgres\`
7. Wait for the async workflow result; only use \`get_workflow_status\` when the tool response did not already include final workflow completion

### Paste Import

1. Parse the user-provided data into schema and rows
2. Call \`import_from_paste\`
3. Wait for the async workflow result; only use \`get_workflow_status\` when the tool response did not already include final workflow completion

## Import Rules

- All imports return an execution ID or execution ARN; for \`get_workflow_status\`, the input field name is \`executionId\`
- When an \`executionArn\` is available, pass that exact ARN string as the \`executionId\` value
- Import tools may already poll asynchronously to final completion; do not immediately call \`get_workflow_status\` again if the tool already returned the completed workflow result
- Pass the returned \`executionArn\` through unchanged as the \`executionId\` value; do not substitute placeholder path params such as \`{id}\` or \`{executionId}\`
- Short execution names are only a fallback for import workflows; for non-import workflows, use the full \`executionArn\`
- For S3 imports, always preview with \`detect_s3_schema\` first
- For Glue cross-account imports, \`sourceAccountId\` is required
- For PostgreSQL, always \`test_connection\` before the import call
- For Snowflake query exploration, call \`detect_connection_schema\` before authoring SQL when identifier casing is unknown
- For \`query_external_connection\`, prefer \`maxRows\` instead of adding \`LIMIT\`, \`TOP\`, or \`FETCH\` in the SQL text
- If Snowflake schema preview shows lowercase identifiers, quote table and column names exactly
- Crawler schedule types: \`none\`, \`hourly\`, \`daily\`, \`weekly\`, \`custom\`
- For one-time imports with no ongoing refresh, use \`crawlerSchedule: { type: "none" }\`; do not invent \`one_time\`, \`once\`, or \`never\`
- Default crawler schedule: \`{ type: "daily" }\`
- If workspace context provides a canonical data lake bucket and AI import staging base prefix, prefer that location for chat-driven uploads and import staging instead of asking the user for another bucket/prefix
- For staged uploads in the DVC bucket, organize paths as \`operations/import/{source-or-domain}/{import-slug}/{YYYY-MM-DD-HH-mm-ss}/\`
- Before starting an import, check whether the destination table already exists when there is any collision risk; use \`get_table_info\` or \`list_tables\` if needed
- If the destination table already exists, tell the user exactly which table exists and ask them to confirm whether to refresh that table or use a different table name
- Do not silently choose a different table name, prefix, or version suffix without the user's confirmation
- After import completes, verify with \`get_table_info\` or \`list_tables\`
- If an upload helper says AWS CLI is missing, verify once with \`aws --version\`; if the CLI is still unavailable but Python credentials work, upload with \`boto3\` instead of stopping
- If a target table already exists or a retry might collide with an existing name, stop and ask whether to refresh the existing table or use a new table name/version suffix
- If an import step or workflow status check fails repeatedly, stop retrying after one unchanged retry and ask the user what to do
- If the same import call fails and the inputs are unchanged, do not retry with renamed tables, alternate file formats, or new staging prefixes until the failure cause is known
- If \`get_workflow_status\` fails twice with the same error, summarize the likely root cause, offer next-step options, and do not keep polling blindly
- When stopping for repeated failures, include the likely causes and a short list of next-step options the user can choose from
- The user may either select one of the options or give an open-ended instruction
- Prefer Parquet when the user controls the export format because schema and typing are more reliable than CSV; keep CSV support for source data that already exists as CSV

## Query vs Import Decision

Not all external data needs to be imported. Use \`query_external_connection\` for:
- One-off exploration or ad-hoc analysis
- Data profiling before committing to import
- Low-volume lookups (reference tables queried occasionally)
- Validating data quality before building a pipeline

Import when:
- Data will be joined with Nile tables in scheduled pipelines
- Multiple users need access to the same external data
- You need versioning, lineage tracking, or scheduled refresh
- You want to build downstream transforms on external data

## Bulk Migration Pattern

When the user wants to import multiple tables:
1. Discover sources
2. Confirm the table list
3. Import sequentially
4. Wait for each workflow to complete before starting the next
5. Summarize imported count, failed count, and total time
`,
};
