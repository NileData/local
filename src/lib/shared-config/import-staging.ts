export interface AiImportStagingPathOptions {
  sourceOrDomain: string;
  importSlug: string;
  timestamp?: Date;
}

export interface AiImportStagingS3UriOptions extends AiImportStagingPathOptions {
  dataLakeBucket: string;
}

function sanitizeSegment(value: string, fallback: string): string {
  const sanitized = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');

  return sanitized || fallback;
}

export function sanitizeImportPathSegment(value: string, fallback = 'import'): string {
  return sanitizeSegment(value, fallback);
}

export function formatAiImportTimestamp(timestamp: Date = new Date()): string {
  const year = timestamp.getUTCFullYear();
  const month = String(timestamp.getUTCMonth() + 1).padStart(2, '0');
  const day = String(timestamp.getUTCDate()).padStart(2, '0');
  const hour = String(timestamp.getUTCHours()).padStart(2, '0');
  const minute = String(timestamp.getUTCMinutes()).padStart(2, '0');
  const second = String(timestamp.getUTCSeconds()).padStart(2, '0');

  return `${year}-${month}-${day}-${hour}-${minute}-${second}`;
}

export function buildAiImportStagingKeyPrefix(
  options: AiImportStagingPathOptions
): string {
  const sourceOrDomain = sanitizeImportPathSegment(options.sourceOrDomain, 'source');
  const importSlug = sanitizeImportPathSegment(options.importSlug, 'import');
  const timestamp = formatAiImportTimestamp(options.timestamp);

  return `operations/import/${sourceOrDomain}/${importSlug}/${timestamp}/`;
}

export function buildAiImportStagingS3Uri(
  options: AiImportStagingS3UriOptions
): string {
  const keyPrefix = buildAiImportStagingKeyPrefix(options);
  return `s3://${options.dataLakeBucket}/${keyPrefix}`;
}
