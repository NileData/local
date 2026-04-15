import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_JSON: SkillDefinition = {
  name: "import-json",
  description: "Import JSON and NDJSON files into Iceberg tables via PySpark.",
  content: `# Import JSON / NDJSON Files

Standard JSON arrays or newline-delimited JSON (NDJSON/JSON Lines). Common for API exports, logs, and event streams.

## Prerequisites

- No extra libraries required -- PySpark handles JSON natively.

## PySpark Recipe

### NDJSON (one JSON object per line -- most common)

\`\`\`python
def transform_data(spark):
    df = spark.read.json("s3://bucket/path/file.jsonl")
    return df
\`\`\`

### JSON Array (single array of objects)

\`\`\`python
def transform_data(spark):
    df = spark.read.option("multiLine", "true").json("s3://bucket/path/file.json")
    return df
\`\`\`

### Nested JSON Flattening

\`\`\`python
def transform_data(spark):
    from pyspark.sql.functions import col, explode

    raw = spark.read.option("multiLine", "true").json("s3://bucket/path/data.json")

    # Flatten nested struct
    df = raw.select(
        col("id"),
        col("name"),
        col("address.street").alias("street"),
        col("address.city").alias("city"),
    )
    return df
\`\`\`

### Explode Arrays

\`\`\`python
def transform_data(spark):
    from pyspark.sql.functions import col, explode

    raw = spark.read.option("multiLine", "true").json("s3://bucket/path/data.json")
    df = raw.select("id", explode("items").alias("item")) \\
        .select("id", "item.name", "item.quantity")
    return df
\`\`\`

## Gotchas

- **Default is NDJSON** -- Spark expects one JSON object per line. For a JSON array file, always set \`multiLine=true\`.
- **Schema inference** -- Spark samples data to infer types. For consistent schemas, define a \`StructType\` explicitly.
- **Corrupt records** -- use \`.option("mode", "PERMISSIVE")\` (default) to capture bad rows in a \`_corrupt_record\` column.
- **Large nested objects** -- deeply nested JSON should be flattened; Iceberg works best with flat schemas.
- **Mixed types** -- if a field has mixed types across records (e.g., string and int), Spark may infer as string.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
