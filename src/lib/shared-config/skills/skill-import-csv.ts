import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_CSV: SkillDefinition = {
  name: "import-csv",
  description: "Import CSV/TSV files into Iceberg tables via PySpark.",
  content: `# Import CSV/TSV Files

Delimited text files (CSV, TSV, pipe-separated). The most common flat-file format for data exchange.

## Prerequisites

- No extra libraries required -- PySpark handles CSV natively.

## PySpark Recipe

\`\`\`python
def transform_data(spark):
    df = (
        spark.read
        .option("header", "true")          # first row is column names
        .option("inferSchema", "true")     # auto-detect types
        .option("delimiter", ",")          # change to "\\t" for TSV, "|" for pipe
        .option("encoding", "UTF-8")       # change if file uses latin1, etc.
        .option("quote", '"')              # quote character
        .option("escape", '"')             # escape character inside quotes
        .option("multiLine", "true")       # allow newlines inside quoted fields
        .option("nullValue", "")           # treat empty strings as null
        .csv("s3://bucket/path/file.csv")
    )
    return df
\`\`\`

### Multiple Files

\`\`\`python
def transform_data(spark):
    df = spark.read.option("header", "true").option("inferSchema", "true") \\
        .csv("s3://bucket/path/")  # reads all CSV files in directory
    return df
\`\`\`

## Gotchas

- **inferSchema reads data twice** -- for large files, define schema explicitly with \`StructType\` instead.
- **Header mismatch across files** -- when reading a directory, all files must have the same header. Use \`mergeSchema\` if they differ.
- **Encoding** -- if you see garbled characters, try \`latin1\`, \`windows-1252\`, or \`iso-8859-1\`.
- **TSV files** -- set \`delimiter\` to \`"\\t"\`. File extension does not matter.
- **Dates** -- use \`.option("dateFormat", "yyyy-MM-dd")\` and \`.option("timestampFormat", "yyyy-MM-dd HH:mm:ss")\` for non-standard formats.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
