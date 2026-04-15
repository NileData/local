import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_DELTA: SkillDefinition = {
  name: "import-delta",
  description: "Import Delta Lake tables into Iceberg tables via PySpark.",
  content: `# Import Delta Lake Tables

Delta Lake tables with ACID transactions and versioning. Read Delta format and convert to Iceberg.

## Prerequisites

- \`delta-spark\` package required (matches your Spark version).
- Delta tables must be accessible via S3 or local path.

## PySpark Recipe

\`\`\`python
def transform_data(spark):
    df = spark.read.format("delta").load("s3://bucket/path/delta-table")
    return df
\`\`\`

### Read Specific Version (Time Travel)

\`\`\`python
def transform_data(spark):
    df = (
        spark.read.format("delta")
        .option("versionAsOf", 5)  # read version 5
        .load("s3://bucket/path/delta-table")
    )
    return df
\`\`\`

### Read as of Timestamp

\`\`\`python
def transform_data(spark):
    df = (
        spark.read.format("delta")
        .option("timestampAsOf", "2025-01-15T00:00:00Z")
        .load("s3://bucket/path/delta-table")
    )
    return df
\`\`\`

### Select Subset of Columns

\`\`\`python
def transform_data(spark):
    df = (
        spark.read.format("delta")
        .load("s3://bucket/path/delta-table")
        .select("id", "name", "amount", "updated_at")
    )
    return df
\`\`\`

## Gotchas

- **Spark version match** -- \`delta-spark\` version must match your Spark version exactly (e.g., delta-spark 3.2.x for Spark 3.5.x).
- **Partition columns** -- Delta partitions are read automatically. No extra config needed.
- **Schema evolution** -- Delta supports schema evolution. The latest schema is used by default.
- **Large tables** -- use \`.select()\` to read only needed columns for performance.
- **Credentials** -- S3 access requires IAM role or credentials configured in Spark session.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
