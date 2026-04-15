import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_EXCEL: SkillDefinition = {
  name: "import-excel",
  description: "Import Excel (.xlsx) files into Iceberg tables via PySpark and openpyxl.",
  content: `# Import Excel (.xlsx) Files

Microsoft Excel workbooks. Supports sheet selection, multi-sheet imports, and named ranges.

## Prerequisites

- Library: \`openpyxl\` (pre-installed in Spark container)
- Verify: \`python -c "import openpyxl; print(openpyxl.__version__)"\`
- PySpark uses \`pandas\` + \`openpyxl\` under the hood via \`spark.createDataFrame(pandas_df)\`.

## PySpark Recipe

\`\`\`python
def transform_data(spark):
    import pandas as pd

    pandas_df = pd.read_excel(
        "/tmp/workbook.xlsx",   # download from S3 first if needed
        sheet_name="Sheet1",    # name or 0-based index
        header=0,               # row index for column names
        dtype=str,              # read everything as string to avoid type loss
    )

    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

### Download from S3 First

\`\`\`python
def transform_data(spark):
    import boto3, pandas as pd

    s3 = boto3.client("s3")
    s3.download_file("bucket", "path/file.xlsx", "/tmp/file.xlsx")

    pandas_df = pd.read_excel("/tmp/file.xlsx", sheet_name=0, dtype=str)
    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

### Multi-Sheet Import

\`\`\`python
def transform_data(spark):
    import pandas as pd

    all_sheets = pd.read_excel("/tmp/file.xlsx", sheet_name=None, dtype=str)
    combined = pd.concat(all_sheets.values(), ignore_index=True)
    df = spark.createDataFrame(combined)
    return df
\`\`\`

## Gotchas

- **File must be local** -- Spark cannot read .xlsx directly from S3. Download to \`/tmp/\` first.
- **dtype=str recommended** -- Excel auto-formats dates and numbers. Read as string, then cast in Spark.
- **Large files** -- openpyxl loads entire workbook into memory. For files > 500MB, consider converting to CSV first.
- **Merged cells** -- openpyxl fills merged cells with \`None\`. Use \`pandas_df.ffill()\` to forward-fill.
- **.xls (legacy)** -- use \`engine="xlrd"\` instead of openpyxl. Install: \`pip install xlrd\`.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
