import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_XML: SkillDefinition = {
  name: "import-xml",
  description: "Import XML files into Iceberg tables via PySpark and lxml.",
  content: `# Import XML Files

XML documents with repeating record elements. Common in legacy systems, SOAP APIs, and regulatory data feeds.

## Prerequisites

- Library: \`lxml\` (pre-installed in Spark container)
- Verify: \`python -c "import lxml; print(lxml.__version__)"\`

## PySpark Recipe

\`\`\`python
def transform_data(spark):
    from lxml import etree
    import pandas as pd

    tree = etree.parse("/tmp/data.xml")
    root = tree.getroot()

    # Extract repeating elements into rows
    rows = []
    for record in root.findall(".//record"):
        row = {child.tag: child.text for child in record}
        rows.append(row)

    pandas_df = pd.DataFrame(rows)
    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

### Namespaced XML

\`\`\`python
def transform_data(spark):
    from lxml import etree
    import pandas as pd

    tree = etree.parse("/tmp/data.xml")
    root = tree.getroot()
    ns = {"ns": "http://example.com/schema"}

    rows = []
    for record in root.findall(".//ns:record", ns):
        row = {child.tag.split("}")[-1]: child.text for child in record}
        rows.append(row)

    pandas_df = pd.DataFrame(rows)
    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

### Attributes + Nested Elements

\`\`\`python
def transform_data(spark):
    from lxml import etree
    import pandas as pd

    tree = etree.parse("/tmp/data.xml")
    rows = []
    for record in tree.findall(".//item"):
        row = dict(record.attrib)                        # attributes
        for child in record:
            row[child.tag] = child.text                  # child elements
        rows.append(row)

    pandas_df = pd.DataFrame(rows)
    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

## Gotchas

- **File must be local** -- download from S3 to \`/tmp/\` first.
- **Identify the repeating element** -- you must know which XML tag represents a "row" (e.g., \`<record>\`, \`<item>\`, \`<row>\`).
- **Namespaces** -- XML namespaces require explicit namespace map. Strip namespace prefixes from column names.
- **Large files** -- for files > 500MB, use \`etree.iterparse()\` for streaming instead of loading the full tree.
- **Mixed content** -- elements with both text and child elements need careful extraction.
- **All values are strings** -- XML text content is always string. Cast columns in Spark after DataFrame creation.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
