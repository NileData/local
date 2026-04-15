import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_PDF: SkillDefinition = {
  name: "import-pdf",
  description: "Import tabular data from PDF files into Iceberg tables via PySpark.",
  content: `# Import PDF Tables

Extract tabular data from PDF documents. Common for financial reports, compliance filings, and regulatory data.

## Prerequisites

- Library: \`pdfplumber\` (needs installation)
- Install: \`pip install pdfplumber\`
- Verify: \`python -c "import pdfplumber; print(pdfplumber.__version__)"\`

Alternative: \`tabula-py\` (requires Java runtime)
- Install: \`pip install tabula-py\`
- Verify: \`python -c "import tabula; print(tabula.__version__)"\`

## PySpark Recipe (pdfplumber)

\`\`\`python
def transform_data(spark):
    import pdfplumber
    import pandas as pd

    rows = []
    with pdfplumber.open("/tmp/report.pdf") as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    rows.append(row)

    # First row as header
    header = rows[0]
    data = rows[1:]

    pandas_df = pd.DataFrame(data, columns=header)
    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

### Specific Pages Only

\`\`\`python
def transform_data(spark):
    import pdfplumber
    import pandas as pd

    rows = []
    with pdfplumber.open("/tmp/report.pdf") as pdf:
        for page in pdf.pages[2:5]:  # pages 3-5 (0-indexed)
            tables = page.extract_tables()
            for table in tables:
                rows.extend(table)

    header = rows[0]
    data = rows[1:]
    pandas_df = pd.DataFrame(data, columns=header)
    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

### Alternative: tabula-py

\`\`\`python
def transform_data(spark):
    import tabula

    dfs = tabula.read_pdf("/tmp/report.pdf", pages="all", multiple_tables=False)
    pandas_df = dfs[0]
    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

## Gotchas

- **File must be local** -- download from S3 to \`/tmp/\` first.
- **Table detection is heuristic** -- PDFs have no semantic table structure. Results depend heavily on formatting.
- **Merged cells** -- spanning cells often produce \`None\` values. Post-process with \`ffill()\` or manual cleanup.
- **Multi-page tables** -- tables spanning pages are extracted per-page. Concatenate and deduplicate headers manually.
- **Scanned PDFs** -- pdfplumber only works on text-based PDFs. Scanned/image PDFs need OCR (Tesseract) first.
- **All values are strings** -- cast columns to proper types after DataFrame creation.
- **pdfplumber vs tabula-py** -- pdfplumber is pure Python (easier install), tabula-py uses Java (better accuracy on complex layouts).

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
