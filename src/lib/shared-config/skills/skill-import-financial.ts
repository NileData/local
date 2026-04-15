import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_FINANCIAL: SkillDefinition = {
  name: "import-financial",
  description: "Import OFX/QFX/QBO bank statement files into Iceberg tables via PySpark.",
  content: `# Import Financial Statement Files (OFX/QFX/QBO)

Bank and financial institution statement files. OFX (Open Financial Exchange), QFX (Quicken), and QBO (QuickBooks) formats all use the same underlying OFX specification.

## Prerequisites

- \`ofxparse\` library required (\`pip install ofxparse\`).

## PySpark Recipe

\`\`\`python
def transform_data(spark):
    import boto3
    from ofxparse import OfxParser

    s3 = boto3.client("s3")
    s3.download_file("bucket", "path/statement.ofx", "/tmp/statement.ofx")

    with open("/tmp/statement.ofx", "rb") as f:
        ofx = OfxParser.parse(f)

    rows = []
    for account in ofx.accounts:
        for txn in account.statement.transactions:
            rows.append({
                "account_id": account.account_id,
                "account_type": str(account.account_type),
                "transaction_id": txn.id,
                "date": str(txn.date.date()),
                "amount": float(txn.amount),
                "type": txn.type,
                "memo": txn.memo or "",
                "payee": txn.payee or "",
            })

    df = spark.createDataFrame(rows)
    return df
\`\`\`

### Multiple Statement Files

\`\`\`python
def transform_data(spark):
    import boto3, os
    from ofxparse import OfxParser

    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket="bucket", Prefix="statements/")

    rows = []
    for obj in resp.get("Contents", []):
        if obj["Key"].endswith((".ofx", ".qfx", ".qbo")):
            local = f"/tmp/{os.path.basename(obj['Key'])}"
            s3.download_file("bucket", obj["Key"], local)
            with open(local, "rb") as f:
                ofx = OfxParser.parse(f)
            for acct in ofx.accounts:
                for txn in acct.statement.transactions:
                    rows.append({
                        "account_id": acct.account_id,
                        "date": str(txn.date.date()),
                        "amount": float(txn.amount),
                        "type": txn.type,
                        "memo": txn.memo or "",
                        "payee": txn.payee or "",
                    })

    df = spark.createDataFrame(rows)
    return df
\`\`\`

## Gotchas

- **File encoding** -- OFX files may use SGML (OFX 1.x) or XML (OFX 2.x). \`ofxparse\` handles both.
- **Date formats** -- dates come as Python datetime objects. Convert to string for Spark.
- **Decimal precision** -- amounts are Python Decimal. Cast to float or store as string to avoid precision loss.
- **Duplicate transactions** -- bank downloads often overlap. Deduplicate by transaction ID + date.
- **QFX/QBO** -- these are just OFX with different extensions. Same parser works for all three.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
