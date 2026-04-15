import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_PARQUET: SkillDefinition = {
  name: "import-parquet",
  description: "Import Parquet files into Iceberg tables via PySpark.",
  content: `# Import Parquet Files

Apache Parquet columnar format. The fastest import path -- schema is embedded, types are preserved, and no inference needed.

## Prerequisites

- No extra libraries required -- PySpark handles Parquet natively.

## PySpark Recipe

\`\`\`python
def transform_data(spark):
    df = spark.read.parquet("s3://bucket/path/file.parquet")
    return df
\`\`\`

### Directory of Parquet Files

\`\`\`python
def transform_data(spark):
    df = spark.read.parquet("s3://bucket/path/")  # reads all .parquet in directory
    return df
\`\`\`

### Partitioned Parquet (Hive-style)

\`\`\`python
def transform_data(spark):
    # Reads s3://bucket/data/year=2024/month=01/*.parquet
    df = spark.read.parquet("s3://bucket/data/")
    # Partition columns (year, month) are auto-discovered
    return df
\`\`\`

### Schema Merge Across Files

\`\`\`python
def transform_data(spark):
    df = spark.read.option("mergeSchema", "true").parquet("s3://bucket/path/")
    return df
\`\`\`

## Gotchas

- **Schema is embedded** -- no need for \`inferSchema\` or header options. Types are always preserved.
- **Schema evolution** -- files with different schemas in the same directory need \`mergeSchema=true\`.
- **Column name case** -- Parquet preserves case but Iceberg lowercases by default. Watch for case mismatches.
- **Decimal precision** -- Parquet stores exact decimal precision. Verify precision/scale match your target Iceberg table.
- **Snappy compression** -- default and handled automatically. No config needed.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
