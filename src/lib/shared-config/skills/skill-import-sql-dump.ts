import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_SQL_DUMP: SkillDefinition = {
  name: "import-sql-dump",
  description: "Import MySQL/PostgreSQL SQL dump files into Iceberg tables via PySpark.",
  content: `# Import SQL Dump Files

SQL dump files from \`mysqldump\` or \`pg_dump\`. Parsed via Python stdlib -- no database connection required.

## Prerequisites

- No extra libraries required -- uses Python \`re\` module for parsing INSERT statements.

## PySpark Recipe

\`\`\`python
def transform_data(spark):
    import re

    # Read the dump file
    with open("/tmp/dump.sql", "r", encoding="utf-8") as f:
        content = f.read()

    # Extract INSERT statements and parse values
    rows = []
    pattern = re.compile(
        r"INSERT INTO [^\s]+ (?:\([^)]+\)\s+)?VALUES\s*(.+?);",
        re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(content):
        values_block = match.group(1)
        for row_match in re.finditer(r"\(([^)]+)\)", values_block):
            raw = row_match.group(1)
            cols = [c.strip().strip("'").replace("\\\\'", "'") for c in raw.split(",")]
            rows.append(cols)

    # Extract column names from CREATE TABLE if available
    create_match = re.search(
        r"CREATE TABLE [^\s]+ \((.+?)\)[^)]*;",
        content, re.IGNORECASE | re.DOTALL,
    )
    if create_match:
        col_defs = create_match.group(1)
        col_names = [
            line.strip().split()[0].strip("\`\"")
            for line in col_defs.split(",")
            if line.strip() and not line.strip().upper().startswith(("PRIMARY", "KEY", "INDEX", "UNIQUE", "CONSTRAINT"))
        ]
    else:
        col_names = [f"col{i}" for i in range(len(rows[0]))]

    df = spark.createDataFrame(rows, col_names)
    return df
\`\`\`

### Download from S3 First

\`\`\`python
import boto3
s3 = boto3.client("s3")
s3.download_file("bucket", "path/dump.sql", "/tmp/dump.sql")
\`\`\`

## Gotchas

- **File must be local** -- download from S3 to \`/tmp/\` before parsing.
- **Large dumps** -- this approach loads the file into memory. For dumps > 1GB, split the file or stream-parse.
- **All columns are strings** -- cast to proper types after creating the DataFrame.
- **MySQL vs PostgreSQL** -- quote styles differ (\`backtick\` vs "double-quote"). The regex handles both.
- **Multi-table dumps** -- filter INSERT statements by table name if the dump contains multiple tables.
- **COPY format (pg_dump)** -- PostgreSQL \`COPY ... FROM stdin\` format needs different parsing (tab-delimited blocks).

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
