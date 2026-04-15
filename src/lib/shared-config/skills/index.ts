export { SKILL_IMPORT_CSV } from "./skill-import-csv.js";
export { SKILL_IMPORT_EXCEL } from "./skill-import-excel.js";
export { SKILL_IMPORT_JSON } from "./skill-import-json.js";
export { SKILL_IMPORT_PARQUET } from "./skill-import-parquet.js";
export { SKILL_IMPORT_AVRO } from "./skill-import-avro.js";
export { SKILL_IMPORT_SQL_DUMP } from "./skill-import-sql-dump.js";
export { SKILL_IMPORT_SQLITE } from "./skill-import-sqlite.js";
export { SKILL_IMPORT_XML } from "./skill-import-xml.js";
export { SKILL_IMPORT_ORC } from "./skill-import-orc.js";
export { SKILL_IMPORT_PDF } from "./skill-import-pdf.js";
export { SKILL_IMPORT_GSHEETS } from "./skill-import-gsheets.js";
export { SKILL_IMPORT_DELTA } from "./skill-import-delta.js";
export { SKILL_IMPORT_GEOJSON } from "./skill-import-geojson.js";
export { SKILL_IMPORT_FINANCIAL } from "./skill-import-financial.js";
export { SKILL_IMPORT_ACCESS } from "./skill-import-access.js";
export { SKILL_IMPORT_LOGS } from "./skill-import-logs.js";
export { SKILL_IMPORT_HTML } from "./skill-import-html.js";
export { SKILL_IMPORT_SHAPEFILE } from "./skill-import-shapefile.js";
export { SKILL_IMPORT_POWERBI } from "./skill-import-powerbi.js";
export { SKILL_IMPORT_CONFIG } from "./skill-import-config.js";
export { SKILL_DATA_PROFILING } from "./skill-data-profiling.js";
export { SKILL_DATA_TRANSFORM } from "./skill-data-transform.js";
export { SKILL_EXPORT } from "./skill-export.js";
export { SKILL_IMPORT_WEB } from "./skill-import-web.js";
export { SKILL_CLOUD_TRANSFER } from "./skill-cloud-transfer.js";
export { SKILL_ENV_SETUP } from "./skill-env-setup.js";

import { SKILL_IMPORT_CSV } from "./skill-import-csv.js";
import { SKILL_IMPORT_EXCEL } from "./skill-import-excel.js";
import { SKILL_IMPORT_JSON } from "./skill-import-json.js";
import { SKILL_IMPORT_PARQUET } from "./skill-import-parquet.js";
import { SKILL_IMPORT_AVRO } from "./skill-import-avro.js";
import { SKILL_IMPORT_SQL_DUMP } from "./skill-import-sql-dump.js";
import { SKILL_IMPORT_SQLITE } from "./skill-import-sqlite.js";
import { SKILL_IMPORT_XML } from "./skill-import-xml.js";
import { SKILL_IMPORT_ORC } from "./skill-import-orc.js";
import { SKILL_IMPORT_PDF } from "./skill-import-pdf.js";
import { SKILL_IMPORT_GSHEETS } from "./skill-import-gsheets.js";
import { SKILL_IMPORT_DELTA } from "./skill-import-delta.js";
import { SKILL_IMPORT_GEOJSON } from "./skill-import-geojson.js";
import { SKILL_IMPORT_FINANCIAL } from "./skill-import-financial.js";
import { SKILL_IMPORT_ACCESS } from "./skill-import-access.js";
import { SKILL_IMPORT_LOGS } from "./skill-import-logs.js";
import { SKILL_IMPORT_HTML } from "./skill-import-html.js";
import { SKILL_IMPORT_SHAPEFILE } from "./skill-import-shapefile.js";
import { SKILL_IMPORT_POWERBI } from "./skill-import-powerbi.js";
import { SKILL_IMPORT_CONFIG } from "./skill-import-config.js";
import { SKILL_DATA_PROFILING } from "./skill-data-profiling.js";
import { SKILL_DATA_TRANSFORM } from "./skill-data-transform.js";
import { SKILL_EXPORT } from "./skill-export.js";
import { SKILL_IMPORT_WEB } from "./skill-import-web.js";
import { SKILL_CLOUD_TRANSFER } from "./skill-cloud-transfer.js";
import { SKILL_ENV_SETUP } from "./skill-env-setup.js";

import type { SkillDefinition } from "../system-skills.js";

/** Format-specific import skills (skills 1-20) */
export const FORMAT_IMPORT_SKILLS: SkillDefinition[] = [
  SKILL_IMPORT_CSV,
  SKILL_IMPORT_EXCEL,
  SKILL_IMPORT_JSON,
  SKILL_IMPORT_PARQUET,
  SKILL_IMPORT_AVRO,
  SKILL_IMPORT_SQL_DUMP,
  SKILL_IMPORT_SQLITE,
  SKILL_IMPORT_XML,
  SKILL_IMPORT_ORC,
  SKILL_IMPORT_PDF,
  SKILL_IMPORT_GSHEETS,
  SKILL_IMPORT_DELTA,
  SKILL_IMPORT_GEOJSON,
  SKILL_IMPORT_FINANCIAL,
  SKILL_IMPORT_ACCESS,
  SKILL_IMPORT_LOGS,
  SKILL_IMPORT_HTML,
  SKILL_IMPORT_SHAPEFILE,
  SKILL_IMPORT_POWERBI,
  SKILL_IMPORT_CONFIG,
];

/** Universal skills available in both cloud and local modes */
export const UNIVERSAL_SKILLS: SkillDefinition[] = [
  SKILL_DATA_PROFILING,
  SKILL_DATA_TRANSFORM,
];

/** Local-only skills */
export const LOCAL_SKILLS: SkillDefinition[] = [
  SKILL_EXPORT,
  SKILL_CLOUD_TRANSFER,
  SKILL_ENV_SETUP,
];

/** Web import skills (both modes) */
export const WEB_IMPORT_SKILLS: SkillDefinition[] = [
  SKILL_IMPORT_WEB,
];

/** All extra skills (skills 11-26) combined */
export const ALL_EXTRA_SKILLS: SkillDefinition[] = [
  ...FORMAT_IMPORT_SKILLS.slice(10), // skills 11-20 (new format skills only)
  ...UNIVERSAL_SKILLS,
  ...LOCAL_SKILLS,
  ...WEB_IMPORT_SKILLS,
];
