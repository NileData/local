export const SYSTEM_SAT_SKILL = {
  name: "sat",
  description: "Save query results as managed tables with refresh jobs.",
  content: `# Save As Table (SAT)

"Save As Table" creates a new managed table from query results. It sets up:
- The table definition with schema inferred from query results
- ETL job(s) to refresh the data on a schedule
- Data lineage tracking based on source tables in the query
- Initial data load from the current query results

## When to Use SAT

Use \`save_query_as_table\` when the user wants to:
- Save query results as a permanent, refreshable table
- Create a scheduled ETL job from an ad-hoc query
- Track lineage for derived or aggregated data

## SAT Decision Logic

### Simple Request

Call the API directly when:
- User says "save this as [table_name]" or "create table [name] from this"
- User provides the table name explicitly
- No complex requirements are mentioned (partitions, custom schedules, dependencies)

Use defaults:
- daily schedule at 12AM local
- no partitions
- \`initialLoad=true\`

### Complex Request

Gather details first when:
- User wants custom scheduling (hourly, specific times, cron)
- User mentions partitioning requirements
- User wants to configure dependencies manually
- User is unsure about configuration

Ask for:
- table name
- schedule preference (daily/hourly/custom cron)
- partition columns if any

Once provided, call \`save_query_as_table\` with the configuration.

## SAT Best Practices

### Reuse Query Execution ID

When the user asks to save query results as a table:
- Use the \`queryExecutionId\` from the last \`get_query_results\` tool response
- Do not re-run the query to get a new execution ID
- The previous execution ID still points to the cached results

### Stringify Nested Columns

When saving query results with arrays, structs, or maps:
- These can cause schema inference issues during table creation
- Use \`to_json()\` or \`cast(... as string)\` on nested columns in SQL

Example:
- Instead of \`SELECT array_col, struct_col FROM ...\`
- Use \`SELECT to_json(array_col) as array_col, to_json(struct_col) as struct_col FROM ...\`

This avoids type mismatches during the initial data load.

## Using save_query_as_table

### Required Parameters

1. \`queryExecutionId\` - from a completed query
2. \`tableName\` - lowercase, letters/numbers/underscores, starts with a letter
3. \`code\`:

\`\`\`json
{
  "content": "SELECT * FROM sales.products",
  "language": "sql",
  "dialect": "sparksql"
}
\`\`\`

4. \`jobs\` - at least one job definition:

\`\`\`json
[{
  "name": "daily_refresh",
  "enabled": true,
  "schedule": { "type": "daily", "cron": "0 0 * * ?" }
}]
\`\`\`

### Optional Parameters

- \`description\` - table description
- \`initialLoad\` - true to load current results (default: true)
- \`dependencies\` - explicit source table list (auto-detected if omitted)

### Example

\`\`\`json
{
  "queryExecutionId": "abc-123-def",
  "tableName": "products_summary",
  "description": "Daily product summary",
  "code": {
    "content": "SELECT category, COUNT(*) as cnt FROM sales.products GROUP BY category",
    "language": "sql",
    "dialect": "sparksql"
  },
  "jobs": [{
    "name": "daily_refresh",
    "enabled": true,
    "schedule": { "type": "daily", "cron": "0 0 * * ?" }
  }],
  "initialLoad": true
}
\`\`\`
`,
};
