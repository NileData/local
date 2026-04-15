import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_HTML: SkillDefinition = {
  name: "import-html",
  description: "Import HTML tables from web pages into Iceberg tables via PySpark.",
  content: `# Import HTML Tables

Extract tabular data from HTML pages. Uses \`pandas.read_html()\` which wraps Python's stdlib html.parser.

## Prerequisites

- No extra libraries required -- \`pandas\` (available in PySpark) handles HTML table parsing.
- For complex pages: \`beautifulsoup4\` + \`lxml\` optional.

## PySpark Recipe

\`\`\`python
def transform_data(spark):
    import pandas as pd

    # Read all tables from a URL or local HTML file
    tables = pd.read_html("https://example.com/data-page.html")

    # tables is a list of DataFrames, pick the one you need
    print(f"Found {len(tables)} tables")
    pandas_df = tables[0]  # first table on the page

    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

### From S3/Local HTML File

\`\`\`python
def transform_data(spark):
    import pandas as pd, boto3

    s3 = boto3.client("s3")
    s3.download_file("bucket", "path/page.html", "/tmp/page.html")

    tables = pd.read_html("/tmp/page.html")
    pandas_df = tables[0]

    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

### Filter by Table Attributes

\`\`\`python
def transform_data(spark):
    import pandas as pd

    # Match table by id, class, or content
    tables = pd.read_html(
        "https://example.com/page.html",
        attrs={"id": "data-table"},     # match by HTML id
        # match="Revenue",              # match tables containing text
    )
    pandas_df = tables[0]

    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

## Gotchas

- **JavaScript-rendered pages** -- \`pd.read_html()\` only sees static HTML. For JS-rendered tables, use a headless browser to save the rendered HTML first.
- **Multiple tables** -- always check \`len(tables)\` and inspect each to find the right one.
- **Header rows** -- use \`header=0\` (default) or \`header=[0,1]\` for multi-level headers.
- **Encoding** -- pass \`encoding="utf-8"\` if you see garbled characters.
- **Merged cells** -- HTML colspan/rowspan create NaN values. Clean with \`fillna()\` or \`ffill()\`.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
