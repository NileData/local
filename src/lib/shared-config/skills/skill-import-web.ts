import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_WEB: SkillDefinition = {
  name: "import-web",
  description: "Import data from web URLs -- fetch CSV/JSON/HTML via HTTP into Iceberg tables.",
  content: `# Import from Web URLs

Fetch data directly from web URLs using Python stdlib. Supports CSV, JSON, and HTML endpoints. For JavaScript-rendered pages, use Playwright MCP for headless browser rendering.

## Prerequisites

- No extra libraries required -- uses Python \`urllib\` (stdlib).
- For JS-rendered sites: Playwright MCP (available in Nile desktop) can render pages first.

## PySpark Recipe (CSV from URL)

\`\`\`python
def transform_data(spark):
    import urllib.request, csv, io

    url = "https://example.com/data.csv"
    response = urllib.request.urlopen(url)
    text = response.read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(text))
    rows = [row for row in reader]

    df = spark.createDataFrame(rows)
    return df
\`\`\`

## JSON API Endpoint

\`\`\`python
def transform_data(spark):
    import urllib.request, json

    url = "https://api.example.com/v1/records?limit=1000"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    response = urllib.request.urlopen(req)
    data = json.loads(response.read().decode("utf-8"))

    # Adjust based on API response structure
    records = data if isinstance(data, list) else data.get("results", data.get("data", []))

    df = spark.createDataFrame(records)
    return df
\`\`\`

## Paginated API

\`\`\`python
def transform_data(spark):
    import urllib.request, json

    all_records = []
    page = 1
    while True:
        url = f"https://api.example.com/v1/records?page={page}&per_page=100"
        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode("utf-8"))
        records = data.get("results", [])
        if not records:
            break
        all_records.extend(records)
        page += 1

    df = spark.createDataFrame(all_records)
    return df
\`\`\`

## With Authentication

\`\`\`python
def transform_data(spark):
    import urllib.request, json, base64

    url = "https://api.example.com/data"
    credentials = base64.b64encode(b"user:password").decode()
    req = urllib.request.Request(url, headers={
        "Authorization": f"Basic {credentials}",
        # Or for Bearer token:
        # "Authorization": "Bearer YOUR_TOKEN_HERE",
    })
    response = urllib.request.urlopen(req)
    data = json.loads(response.read().decode("utf-8"))

    df = spark.createDataFrame(data)
    return df
\`\`\`

## Gotchas

- **SSL certificates** -- some corporate environments require custom CA bundles. Use \`ssl.create_default_context(cafile=...)\` if needed.
- **Rate limiting** -- add \`time.sleep()\` between paginated requests to respect API rate limits.
- **JS-rendered pages** -- \`urllib\` only fetches raw HTML. For SPAs or JS-rendered content, use Playwright MCP to render the page and extract data from the DOM.
- **Large responses** -- for responses > 100MB, stream with \`response.read(chunk_size)\` instead of reading all at once.
- **Encoding** -- check \`response.headers.get_content_charset()\` for correct decoding.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
