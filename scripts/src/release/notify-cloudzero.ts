// oxlint-disable no-console

import {assertZeroVersion, mustEnv, zeroGhcrImage} from '../shared.ts';

const targetStages = ['STAGING', 'PROD'] as const;

export type NotifyTargetStage = (typeof targetStages)[number];

export type NotifyTarget = {
  stage: NotifyTargetStage;
  url: string;
  token: string;
};

export type NotifyFetch = (
  url: string,
  init: {
    method: string;
    headers: Record<string, string>;
    body: string;
    signal: AbortSignal;
  },
) => Promise<{ok: boolean; status: number; text(): Promise<string>}>;

export type NotifyCloudZeroOptions = {
  attempts?: number | undefined;
  env?: NodeJS.ProcessEnv | undefined;
  fetchImpl?: NotifyFetch | undefined;
  log?: ((message: string) => void) | undefined;
  retryDelayMs?: number | undefined;
  sleep?: ((ms: number) => Promise<void>) | undefined;
  timeoutMs?: number | undefined;
  version: string;
};

export type NotifyCloudZeroResult = {
  imageUri: string;
  notified: NotifyTargetStage[];
  failed: NotifyTargetStage[];
};

export async function runReleaseNotifyCloudZeroCli() {
  const {imageUri, failed} = await notifyCloudZero({
    version: mustEnv('VERSION'),
  });
  if (failed.length > 0) {
    throw new Error(
      `Failed to notify Cloud Zero of ${imageUri}: ${failed.join(', ')}`,
    );
  }
}

export function readNotifyTargets(env: NodeJS.ProcessEnv): NotifyTarget[] {
  const targets: NotifyTarget[] = [];
  for (const stage of targetStages) {
    const url = env[`CLOUDZERO_IMAGE_RELEASER_URL_${stage}`];
    const token = env[`CLOUDZERO_IMAGE_RELEASER_TOKEN_${stage}`];
    if (!url && !token) {
      continue;
    }
    if (!url || !token) {
      throw new Error(
        `CLOUDZERO_IMAGE_RELEASER_URL_${stage} and CLOUDZERO_IMAGE_RELEASER_TOKEN_${stage} must be configured together`,
      );
    }
    targets.push({stage, url, token});
  }
  return targets;
}

export async function notifyCloudZero({
  attempts = 3,
  env = process.env,
  fetchImpl = fetch,
  log = console.log,
  retryDelayMs = 2_000,
  sleep = ms => new Promise(resolve => setTimeout(resolve, ms)),
  timeoutMs = 10_000,
  version,
}: NotifyCloudZeroOptions): Promise<NotifyCloudZeroResult> {
  assertZeroVersion(version);
  const imageUri = `${zeroGhcrImage}:${version}`;
  const targets = readNotifyTargets(env);
  if (targets.length === 0) {
    log('No Cloud Zero image releaser targets configured; skipping.');
    return {imageUri, notified: [], failed: []};
  }

  const notified: NotifyTargetStage[] = [];
  const failed: NotifyTargetStage[] = [];
  for (const target of targets) {
    const ok = await notifyTarget({
      attempts,
      fetchImpl,
      imageUri,
      log,
      retryDelayMs,
      sleep,
      target,
      timeoutMs,
    });
    (ok ? notified : failed).push(target.stage);
  }
  return {imageUri, notified, failed};
}

async function notifyTarget({
  attempts,
  fetchImpl,
  imageUri,
  log,
  retryDelayMs,
  sleep,
  target,
  timeoutMs,
}: {
  attempts: number;
  fetchImpl: NotifyFetch;
  imageUri: string;
  log: (message: string) => void;
  retryDelayMs: number;
  sleep: (ms: number) => Promise<void>;
  target: NotifyTarget;
  timeoutMs: number;
}): Promise<boolean> {
  for (let attempt = 1; attempt <= attempts; attempt++) {
    if (attempt > 1) {
      await sleep(retryDelayMs * (attempt - 1));
    }
    try {
      const response = await fetchImpl(target.url, {
        method: 'POST',
        headers: {
          'authorization': `Bearer ${target.token}`,
          'content-type': 'application/json',
        },
        body: JSON.stringify({channel: 'head', imageUri}),
        signal: AbortSignal.timeout(timeoutMs),
      });
      const body = (await response.text()).trim();
      if (response.ok) {
        log(`Notified Cloud Zero ${target.stage} of ${imageUri}: ${body}`);
        return true;
      }
      log(
        `Cloud Zero ${target.stage} responded ${response.status} for ${imageUri}: ${body}`,
      );
      if (response.status < 500) {
        // 4xx (bad token, rejected payload) will not heal on retry.
        return false;
      }
    } catch (error) {
      log(`Cloud Zero ${target.stage} request failed: ${String(error)}`);
    }
  }
  log(`Giving up on Cloud Zero ${target.stage} after ${attempts} attempts`);
  return false;
}
