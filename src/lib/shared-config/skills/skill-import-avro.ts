import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_AVRO: SkillDefinition = {
  name: "import-avro",
  description: "Import Avro files into Iceberg tables via PySpark.",
  content: `# Import Avro Files

Apache Avro row-based format with embedded schema. Common in Kafka pipelines, Hadoop ecosystems, and schema-registry workflows.

## Prerequisites

- No extra libraries required -- Spark includes Avro support via \`spark-avro\` (built-in since Spark 2.4).

## PySpark Recipe

\`\`\`python
def transform_data(spark):
    df = spark.read.format("avro").load("s3://bucket/path/file.avro")
    return df
\`\`\`

### Directory of Avro Files

\`\`\`python
def transform_data(spark):
    df = spark.read.format("avro").load("s3://bucket/path/")
    return df
\`\`\`

### With Explicit Avro Schema

\`\`\`python
def transform_data(spark):
    avro_schema = open("/tmp/schema.avsc").read()
    df = (
        spark.read
        .format("avro")
        .option("avroSchema", avro_schema)
        .load("s3://bucket/path/")
    )
    return df
\`\`\`

## Gotchas

- **Schema evolution** -- Avro supports adding/removing fields with defaults. Use \`avroSchema\` option to read with a specific reader schema different from the writer schema.
- **Union types** -- Avro unions (e.g., \`["null", "string"]\`) map to nullable Spark columns. Complex unions (3+ types) may need manual handling.
- **Logical types** -- Avro \`date\`, \`timestamp-millis\`, and \`decimal\` map to Spark types automatically.
- **Enum fields** -- Avro enums are read as strings in Spark.
- **Format keyword** -- must use \`.format("avro").load()\` not \`.avro()\` shorthand.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
