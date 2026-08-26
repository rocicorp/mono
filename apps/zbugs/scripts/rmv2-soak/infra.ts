import {spawn} from 'node:child_process';
import {
  CreateBucketCommand,
  DeleteObjectsCommand,
  ListObjectsV2Command,
  S3Client,
} from '@aws-sdk/client-s3';
import {DOCKER_DIR, type SoakConfig} from './config.ts';

const COMPOSE_FILES = [
  '-f',
  'docker-compose.yml',
  '-f',
  'docker-compose.minio.yml',
];

export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function run(
  command: string,
  args: readonly string[],
  cwd: string,
): Promise<{code: number; stdout: string; stderr: string}> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    child.stdout?.on('data', c => (stdout += String(c)));
    child.stderr?.on('data', c => (stderr += String(c)));
    child.once('error', reject);
    child.once('exit', code => resolve({code: code ?? -1, stdout, stderr}));
  });
}

async function compose(...args: string[]): Promise<string> {
  const {code, stdout, stderr} = await run(
    'docker',
    ['compose', ...COMPOSE_FILES, ...args],
    DOCKER_DIR,
  );
  if (code !== 0) {
    throw new Error(
      `docker compose ${args.join(' ')} exited with ${code}\n${stderr}`,
    );
  }
  return stdout;
}

/** Brings up postgres and minio, and waits for both to answer. */
export async function startInfra(config: SoakConfig): Promise<void> {
  await compose('up', '-d', 'postgres_primary', 'minio');
  await waitForMinio(config, 120_000);
  // The bucket-init container exits 0 once the bucket is present.
  await compose('up', '--exit-code-from', 'minio_init', 'minio_init');
}

export async function stopMinio(): Promise<void> {
  await compose('stop', 'minio');
}

export async function startMinio(config: SoakConfig): Promise<void> {
  await compose('start', 'minio');
  await waitForMinio(config, 120_000);
}

export async function waitForMinio(
  config: SoakConfig,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(
        new URL('/minio/health/live', config.s3Endpoint),
      );
      if (response.ok) {
        return;
      }
      lastError = new Error(`HTTP ${response.status}`);
    } catch (e) {
      lastError = e;
    }
    await sleep(500);
  }
  throw new Error(`minio did not become healthy: ${String(lastError)}`);
}

export function s3Client(config: SoakConfig): S3Client {
  return new S3Client({
    endpoint: config.s3Endpoint,
    region: config.s3Region,
    // minio serves virtual-host style only with DNS wildcards, which a
    // localhost endpoint has no way to provide.
    forcePathStyle: true,
    credentials: {
      accessKeyId: config.accessKeyID,
      secretAccessKey: config.secretAccessKey,
    },
  });
}

/**
 * The bucket the soak's backups live in.
 *
 * Only the bucket, deliberately: the replication-manager does not publish to
 * the configured path. `initializePostgresChangeSource` derives a
 * *destination* backup URL whose last segment is a generation id -- the
 * replica fork/resumption identity -- and logs it as `setting up backup to
 * s3://<bucket>/<generation>`. The configured path is therefore not a prefix
 * of anything, and the soak owns the whole bucket instead.
 */
export function backupBucket(backupURL: string): string {
  const url = new URL(backupURL);
  if (url.protocol !== 's3:') {
    throw new Error(`expected an s3:// backup URL, got ${backupURL}`);
  }
  return url.hostname;
}

/**
 * Empties the backup bucket so that a run starts from a clean generation. A
 * leftover backup from a previous run carries a different `replicaVersion`
 * and would be rejected rather than restored, which shows up as a confusing
 * initial sync rather than as an error.
 */
export async function resetBackup(config: SoakConfig): Promise<number> {
  const client = s3Client(config);
  const bucket = backupBucket(config.backupURL);
  try {
    await client.send(new CreateBucketCommand({Bucket: bucket}));
  } catch {
    // Already exists.
  }
  let deleted = 0;
  let token: string | undefined;
  do {
    const page = await client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        ContinuationToken: token,
      }),
    );
    const keys = (page.Contents ?? [])
      .map(o => o.Key)
      .filter((k): k is string => k !== undefined);
    if (keys.length > 0) {
      await client.send(
        new DeleteObjectsCommand({
          Bucket: bucket,
          Delete: {Objects: keys.map(Key => ({Key}))},
        }),
      );
      deleted += keys.length;
    }
    token = page.IsTruncated ? page.NextContinuationToken : undefined;
  } while (token);
  client.destroy();
  return deleted;
}

/** Total bytes and object count currently held in the backup bucket. */
export async function backupSize(
  config: SoakConfig,
): Promise<{objects: number; bytes: number}> {
  const client = s3Client(config);
  const bucket = backupBucket(config.backupURL);
  let objects = 0;
  let bytes = 0;
  let token: string | undefined;
  try {
    do {
      const page = await client.send(
        new ListObjectsV2Command({
          Bucket: bucket,
          ContinuationToken: token,
        }),
      );
      for (const o of page.Contents ?? []) {
        objects++;
        bytes += o.Size ?? 0;
      }
      token = page.IsTruncated ? page.NextContinuationToken : undefined;
    } while (token);
  } finally {
    client.destroy();
  }
  return {objects, bytes};
}
