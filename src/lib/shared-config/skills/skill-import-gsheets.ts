import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_GSHEETS: SkillDefinition = {
  name: "import-gsheets",
  description: "Import Google Sheets into Iceberg tables via PySpark.",
  content: `# Import Google Sheets

Google Sheets spreadsheets. Export as CSV/XLSX and read with PySpark, or use the Sheets API for programmatic access.

## Prerequisites

- No extra libraries required for the export-as-CSV approach.
- For API access: \`google-api-python-client\`, \`google-auth\` (service account credentials needed).

## PySpark Recipe (Export as CSV)

The simplest approach: publish the sheet to web as CSV, then read directly.

\`\`\`python
def transform_data(spark):
    import urllib.request, csv, io
    from pyspark.sql import Row
    from pyspark.sql.types import StructType, StructField, StringType

    # Use the "Publish to web" CSV URL from Google Sheets
    # Format: https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}
    url = "https://docs.google.com/spreadsheets/d/SHEET_ID/export?format=csv&gid=0"

    response = urllib.request.urlopen(url)
    text = response.read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    rows = [row for row in reader]

    df = spark.createDataFrame(rows)
    return df
\`\`\`

### Via Downloaded XLSX

\`\`\`python
def transform_data(spark):
    import pandas as pd

    # Download XLSX export from Google Sheets (or from S3 after manual export)
    pandas_df = pd.read_excel("/tmp/sheet_export.xlsx", sheet_name="Sheet1")
    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

## Gotchas

- **Published sheets only** -- the CSV export URL requires the sheet to be published to web or shared with "anyone with the link".
- **Large sheets** -- Google Sheets has a 10 million cell limit. For very large sheets, export as XLSX first.
- **Multiple tabs** -- change the \`gid\` parameter to target different tabs (gid=0 is the first tab).
- **Type inference** -- all columns arrive as strings from CSV export. Cast explicitly in Spark.
- **Rate limits** -- Google may rate-limit repeated downloads. Cache locally for dev/testing.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
