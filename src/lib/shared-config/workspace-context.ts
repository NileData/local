export interface DvcWorkspaceContextOptions {
  awsAccountId?: string;
  awsRegion?: string;
  dataLakeBucket?: string;
  aiImportPrefix?: string;
}

export function getDefaultAiImportPrefix(dataLakeBucket: string): string {
  return `s3://${dataLakeBucket}/operations/import/`;
}

export function buildDvcWorkspaceContext(options: DvcWorkspaceContextOptions = {}): string {
  const lines: string[] = [];

  if (options.awsAccountId || options.awsRegion) {
    const parts = [
      options.awsAccountId ? `account ${options.awsAccountId}` : '',
      options.awsRegion ? `region ${options.awsRegion}` : '',
    ].filter(Boolean);

    if (parts.length > 0) {
      lines.push(`- Deployment: ${parts.join(', ')}`);
    }
  }

  if (options.dataLakeBucket) {
    lines.push(`- Canonical data lake bucket: \`s3://${options.dataLakeBucket}/\``);
  }

  const aiImportPrefix = options.aiImportPrefix
    || (options.dataLakeBucket ? getDefaultAiImportPrefix(options.dataLakeBucket) : undefined);
  if (aiImportPrefix) {
    lines.push(`- Preferred AI import staging base prefix: \`${aiImportPrefix}\``);
    lines.push('- For chat-driven staged imports, use `operations/import/{source-or-domain}/{import-slug}/{YYYY-MM-DD-HH-mm-ss}/` under that bucket unless the user explicitly wants a different source location');
  }

  return lines.join('\n');
}
