import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_ORC: SkillDefinition = {
  name: "import-orc",
  description: "Import ORC files into Iceberg tables via PySpark.",
  content: `# Import ORC Files

Apache ORC (Optimized Row Columnar) format. Common in Hive/Hadoop ecosystems with built-in compression and predicate pushdown.

## Prerequisites

- No extra libraries required -- PySpark handles ORC natively (Spark built-in).

## PySpark Recipe

\`\`\`python
def transform_data(spark):
    df = spark.read.orc("s3://bucket/path/file.orc")
    return df
\`\`\`

### Directory of ORC Files

\`\`\`python
def transform_data(spark):
    df = spark.read.orc("s3://bucket/path/")  # reads all .orc files in directory
    return df
\`\`\`

### Partitioned ORC (Hive-style)

\`\`\`python
def transform_data(spark):
    # Reads s3://bucket/data/year=2024/month=01/*.orc
    df = spark.read.orc("s3://bucket/data/")
    # Partition columns are auto-discovered
    return df
\`\`\`

### Schema Merge

\`\`\`python
def transform_data(spark):
    df = spark.read.option("mergeSchema", "true").orc("s3://bucket/path/")
    return df
\`\`\`

## Gotchas

- **Schema is embedded** -- like Parquet, no inference needed. Types are preserved exactly.
- **Hive compatibility** -- ORC files created by Hive use Hive SerDe. Spark reads these natively.
- **ACID tables** -- ORC ACID tables from Hive 3.x include delta directories. Spark may need \`mergeSchema\` or explicit path filtering to skip delete-delta files.
- **Bloom filters** -- ORC bloom filter indexes are read-side only and do not affect import behavior.
- **Compression** -- ORC defaults to ZLIB compression. Spark handles all ORC compression codecs automatically (NONE, ZLIB, SNAPPY, LZO, LZ4, ZSTD).
- **Timestamps** -- ORC stores timestamps in UTC. Verify timezone handling matches your requirements.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
