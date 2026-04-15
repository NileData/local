import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_SQLITE: SkillDefinition = {
  name: "import-sqlite",
  description: "Import SQLite database files into Iceberg tables via PySpark.",
  content: `# Import SQLite Files

SQLite \`.db\`/\`.sqlite\` database files. Read via Python \`sqlite3\` stdlib module + pandas bridge to Spark.

## Prerequisites

- No extra libraries required -- Python \`sqlite3\` is in the standard library.

## PySpark Recipe

\`\`\`python
def transform_data(spark):
    import sqlite3
    import pandas as pd

    conn = sqlite3.connect("/tmp/database.db")
    pandas_df = pd.read_sql("SELECT * FROM target_table", conn)
    conn.close()

    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

### Download from S3 First

\`\`\`python
def transform_data(spark):
    import boto3, sqlite3, pandas as pd

    s3 = boto3.client("s3")
    s3.download_file("bucket", "path/database.db", "/tmp/database.db")

    conn = sqlite3.connect("/tmp/database.db")
    pandas_df = pd.read_sql("SELECT * FROM target_table", conn)
    conn.close()

    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

### List Available Tables

\`\`\`python
conn = sqlite3.connect("/tmp/database.db")
tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
print(tables)
conn.close()
\`\`\`

### Import with Custom Query

\`\`\`python
def transform_data(spark):
    import sqlite3, pandas as pd

    conn = sqlite3.connect("/tmp/database.db")
    pandas_df = pd.read_sql("""
        SELECT id, name, created_at
        FROM users
        WHERE active = 1
        ORDER BY created_at DESC
    """, conn)
    conn.close()

    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

## Gotchas

- **File must be local** -- SQLite cannot read from S3 directly. Download to \`/tmp/\` first.
- **Type mapping** -- SQLite has loose typing. Pandas infers types; cast explicitly in Spark if needed.
- **Large databases** -- for databases > 1GB, use chunked reads: \`pd.read_sql(..., chunksize=100000)\` and union the DataFrames.
- **WAL mode** -- if the database uses WAL journaling, ensure both \`.db\` and \`.db-wal\` files are downloaded.
- **BLOB columns** -- binary data columns need special handling or should be excluded from the import.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
