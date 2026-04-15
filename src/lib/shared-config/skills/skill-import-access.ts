import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_ACCESS: SkillDefinition = {
  name: "import-access",
  description: "Import Microsoft Access (.mdb/.accdb) databases into Iceberg tables via PySpark.",
  content: `# Import Microsoft Access Databases

Microsoft Access \`.mdb\` (Access 2003 and earlier) and \`.accdb\` (Access 2007+) database files. Legacy Windows database format common in enterprise environments.

## Prerequisites

- \`pyodbc\` with Microsoft Access ODBC driver, OR
- \`mdbtools\` (open-source, Linux-compatible) -- often easier in cloud/EMR environments.
- For mdbtools approach: \`subprocess\` (stdlib) to call \`mdb-export\`.

## PySpark Recipe (mdbtools)

\`\`\`python
def transform_data(spark):
    import subprocess, csv, io, boto3

    s3 = boto3.client("s3")
    s3.download_file("bucket", "path/database.accdb", "/tmp/database.accdb")

    # List tables in the database
    tables_output = subprocess.check_output(["mdb-tables", "-1", "/tmp/database.accdb"]).decode()
    print("Available tables:", tables_output)

    # Export target table to CSV
    csv_output = subprocess.check_output(
        ["mdb-export", "/tmp/database.accdb", "TargetTable"]
    ).decode("utf-8")

    reader = csv.DictReader(io.StringIO(csv_output))
    rows = [row for row in reader]

    df = spark.createDataFrame(rows)
    return df
\`\`\`

### Via pyodbc (Windows/ODBC driver available)

\`\`\`python
def transform_data(spark):
    import pyodbc, pandas as pd

    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        r"DBQ=/tmp/database.accdb;"
    )
    conn = pyodbc.connect(conn_str)
    pandas_df = pd.read_sql("SELECT * FROM TargetTable", conn)
    conn.close()

    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

## Gotchas

- **mdbtools availability** -- install via \`apt-get install mdbtools\` on Debian/Ubuntu. Not available on all EMR AMIs.
- **Character encoding** -- Access databases may use Windows-1252 encoding. Decode accordingly.
- **OLE/BLOB fields** -- binary and OLE object columns cannot be exported via mdb-export. Exclude them.
- **Relationships** -- Access relationships are not exported. Join tables manually in Spark if needed.
- **Password-protected** -- mdbtools does not support password-protected databases. Use pyodbc with the password parameter.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
