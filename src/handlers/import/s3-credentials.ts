import { fromNodeProviderChain } from '@aws-sdk/credential-providers';
import type { SparkEngine } from '../../engines/spark-engine.js';

/**
 * Resolve AWS credentials from the host's default credential chain
 * (SSO, profiles, env vars, instance metadata) and inject them into
 * the Spark session's Hadoop S3A config via SET commands.
 *
 * Called on-demand before S3 reads -- no global side effects, no
 * process.env mutation. Only affects the current Spark session.
 *
 * Throws if credentials cannot be resolved so the import fails with
 * a clear error instead of a cryptic Hadoop "No AWS Credentials" message.
 */
export async function configureSparkS3Credentials(sparkEngine: SparkEngine): Promise<void> {
  const provider = fromNodeProviderChain();
  const creds = await provider();

  await sparkEngine.executeDDL(
    `SET spark.hadoop.fs.s3a.access.key=${creds.accessKeyId}`
  );
  await sparkEngine.executeDDL(
    `SET spark.hadoop.fs.s3a.secret.key=${creds.secretAccessKey}`
  );
  if (creds.sessionToken) {
    await sparkEngine.executeDDL(
      `SET spark.hadoop.fs.s3a.session.token=${creds.sessionToken}`
    );
    // Switch to temp credential provider when session token is present
    await sparkEngine.executeDDL(
      `SET spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider`
    );
  }
}
