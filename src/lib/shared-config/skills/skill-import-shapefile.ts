import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_SHAPEFILE: SkillDefinition = {
  name: "import-shapefile",
  description: "Import ESRI Shapefile (.shp) GIS data into Iceberg tables via PySpark.",
  content: `# Import Shapefiles

ESRI Shapefile format -- the most widely used format for GIS vector data. A "shapefile" is actually a set of files (.shp, .shx, .dbf, .prj, and optionally others).

## Prerequisites

- \`geopandas\` and \`fiona\` required (\`pip install geopandas fiona\`).
- All component files (.shp, .shx, .dbf, .prj) must be present together.

## PySpark Recipe

\`\`\`python
def transform_data(spark):
    import geopandas as gpd, boto3, os

    s3 = boto3.client("s3")
    extensions = [".shp", ".shx", ".dbf", ".prj", ".cpg"]
    for ext in extensions:
        key = f"path/data{ext}"
        try:
            s3.download_file("bucket", key, f"/tmp/data{ext}")
        except Exception:
            pass  # .cpg is optional

    gdf = gpd.read_file("/tmp/data.shp")
    # Convert geometry to WKT for Iceberg storage
    gdf["geometry_wkt"] = gdf.geometry.to_wkt()
    pandas_df = gdf.drop(columns=["geometry"])

    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

### Read from ZIP Archive

\`\`\`python
def transform_data(spark):
    import geopandas as gpd, boto3

    s3 = boto3.client("s3")
    s3.download_file("bucket", "path/shapefile.zip", "/tmp/shapefile.zip")

    # geopandas can read shapefiles directly from ZIP
    gdf = gpd.read_file("zip:///tmp/shapefile.zip")
    gdf["geometry_wkt"] = gdf.geometry.to_wkt()
    pandas_df = gdf.drop(columns=["geometry"])

    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

## Gotchas

- **File bundle** -- a shapefile is multiple files. Missing .shx or .dbf will cause read failures.
- **Coordinate system** -- check the .prj file for CRS. Reproject with \`gdf.to_crs(epsg=4326)\` if needed.
- **Field name truncation** -- DBF format limits column names to 10 characters. Rename after loading.
- **Encoding** -- attribute data may use non-UTF8 encoding. Set \`encoding\` parameter in \`read_file()\`.
- **Large shapefiles** -- for files > 1GB, consider reading in chunks or converting to GeoParquet first.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
