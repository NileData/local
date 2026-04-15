import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_LOGS: SkillDefinition = {
  name: "import-logs",
  description: "Import log files (Apache, Nginx, syslog, custom) into Iceberg tables via PySpark.",
  content: `# Import Log Files

Parse structured and semi-structured log files using regex patterns. Supports Apache/Nginx access logs, syslog, and custom formats.

## Prerequisites

- No extra libraries required -- uses PySpark and Python \`re\` module.

## PySpark Recipe (Apache/Nginx Combined Log)

\`\`\`python
def transform_data(spark):
    from pyspark.sql.functions import regexp_extract, col

    raw = spark.read.text("s3://bucket/path/access.log")

    # Apache/Nginx combined log format pattern
    pattern = r'^(\\S+) \\S+ \\S+ \\[([^]]+)\\] "(\\S+) (\\S+) \\S+" (\\d{3}) (\\d+|-) "(.*?)" "(.*?)"'

    df = raw.select(
        regexp_extract("value", pattern, 1).alias("ip"),
        regexp_extract("value", pattern, 2).alias("timestamp"),
        regexp_extract("value", pattern, 3).alias("method"),
        regexp_extract("value", pattern, 4).alias("path"),
        regexp_extract("value", pattern, 5).cast("int").alias("status"),
        regexp_extract("value", pattern, 6).cast("int").alias("bytes"),
        regexp_extract("value", pattern, 7).alias("referer"),
        regexp_extract("value", pattern, 8).alias("user_agent"),
    ).filter(col("ip") != "")

    return df
\`\`\`

### Syslog Format

\`\`\`python
def transform_data(spark):
    from pyspark.sql.functions import regexp_extract, col

    raw = spark.read.text("s3://bucket/path/syslog")

    # Syslog pattern: "Mon DD HH:MM:SS hostname process[pid]: message"
    pattern = r'^(\\w{3}\\s+\\d{1,2}\\s+\\d{2}:\\d{2}:\\d{2}) (\\S+) (\\S+?)(?:\\[(\\d+)\\])?: (.+)$'

    df = raw.select(
        regexp_extract("value", pattern, 1).alias("timestamp"),
        regexp_extract("value", pattern, 2).alias("hostname"),
        regexp_extract("value", pattern, 3).alias("process"),
        regexp_extract("value", pattern, 4).alias("pid"),
        regexp_extract("value", pattern, 5).alias("message"),
    ).filter(col("timestamp") != "")

    return df
\`\`\`

### JSON-Line Logs (structured)

\`\`\`python
def transform_data(spark):
    df = spark.read.json("s3://bucket/path/app-logs/")
    return df
\`\`\`

## Gotchas

- **Multiline logs** -- stack traces span multiple lines. Use \`wholeTextFiles\` and split by log entry boundary.
- **Regex mismatches** -- filter out unparseable lines (\`filter(col("ip") != "")\`) to avoid null rows.
- **Timestamps** -- log timestamp formats vary. Parse with \`to_timestamp(col, format)\` after extraction.
- **Compressed logs** -- PySpark reads \`.gz\` files natively. No decompression needed.
- **Mixed formats** -- if log format changed over time, union separate DataFrames with different patterns.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
