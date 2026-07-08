import {expect, test, vi} from 'vitest';
import {
  notifyCloudZero,
  readNotifyTargets,
  type NotifyFetch,
} from './notify-cloudzero.ts';

const version = '1.8.0-head.202607082153';
const imageUri = `ghcr.io/rocicorp/zero:${version}`;

test('readNotifyTargets requires url and token together', () => {
  expect(readNotifyTargets({})).toEqual([]);

  expect(
    readNotifyTargets({
      CLOUDZERO_IMAGE_RELEASER_URL_STAGING: 'https://staging.example',
      CLOUDZERO_IMAGE_RELEASER_TOKEN_STAGING: 'staging-token',
      CLOUDZERO_IMAGE_RELEASER_URL_PROD: 'https://prod.example',
      CLOUDZERO_IMAGE_RELEASER_TOKEN_PROD: 'prod-token',
    }),
  ).toEqual([
    {stage: 'STAGING', url: 'https://staging.example', token: 'staging-token'},
    {stage: 'PROD', url: 'https://prod.example', token: 'prod-token'},
  ]);

  expect(() =>
    readNotifyTargets({
      CLOUDZERO_IMAGE_RELEASER_URL_STAGING: 'https://staging.example',
    }),
  ).toThrowErrorMatchingInlineSnapshot(
    `[Error: CLOUDZERO_IMAGE_RELEASER_URL_STAGING and CLOUDZERO_IMAGE_RELEASER_TOKEN_STAGING must be configured together]`,
  );
});

test('notifyCloudZero skips green when no targets are configured', async () => {
  const log = vi.fn();
  const fetchImpl = vi.fn();

  await expect(
    notifyCloudZero({env: {}, fetchImpl, log, version}),
  ).resolves.toEqual({imageUri, notified: [], failed: []});

  expect(fetchImpl).not.toHaveBeenCalled();
  expect(log).toHaveBeenCalledWith(
    'No Cloud Zero image releaser targets configured; skipping.',
  );
});

test('notifyCloudZero rejects invalid versions', async () => {
  await expect(
    notifyCloudZero({env: {}, version: 'not-a-version'}),
  ).rejects.toThrowErrorMatchingInlineSnapshot(
    `[Error: Invalid version not-a-version]`,
  );
});

test('notifyCloudZero posts the release to every configured target', async () => {
  const calls: Array<{url: string; init: Parameters<NotifyFetch>[1]}> = [];
  const fetchImpl: NotifyFetch = (url, init) => {
    calls.push({url, init});
    return Promise.resolve(okResponse('{"ok":true,"stacksUpdated":2}'));
  };

  await expect(
    notifyCloudZero({
      env: {
        CLOUDZERO_IMAGE_RELEASER_URL_STAGING: 'https://staging.example',
        CLOUDZERO_IMAGE_RELEASER_TOKEN_STAGING: 'staging-token',
        CLOUDZERO_IMAGE_RELEASER_URL_PROD: 'https://prod.example',
        CLOUDZERO_IMAGE_RELEASER_TOKEN_PROD: 'prod-token',
      },
      fetchImpl,
      log: vi.fn(),
      version,
    }),
  ).resolves.toEqual({imageUri, notified: ['STAGING', 'PROD'], failed: []});

  expect(calls.map(({url}) => url)).toEqual([
    'https://staging.example',
    'https://prod.example',
  ]);
  expect(calls[0].init.method).toBe('POST');
  expect(calls[0].init.headers).toEqual({
    'authorization': 'Bearer staging-token',
    'content-type': 'application/json',
  });
  expect(JSON.parse(calls[0].init.body)).toEqual({
    channel: 'head',
    imageUri,
  });
  expect(calls[1].init.headers.authorization).toBe('Bearer prod-token');
});

test('notifyCloudZero retries server errors with backoff and reports failures', async () => {
  const sleeps: number[] = [];
  let stagingAttempts = 0;
  const fetchImpl: NotifyFetch = url => {
    if (url === 'https://staging.example') {
      stagingAttempts++;
      return Promise.resolve(errorResponse(503, 'unavailable'));
    }
    return Promise.resolve(okResponse('{"ok":true}'));
  };

  await expect(
    notifyCloudZero({
      env: {
        CLOUDZERO_IMAGE_RELEASER_URL_STAGING: 'https://staging.example',
        CLOUDZERO_IMAGE_RELEASER_TOKEN_STAGING: 'staging-token',
        CLOUDZERO_IMAGE_RELEASER_URL_PROD: 'https://prod.example',
        CLOUDZERO_IMAGE_RELEASER_TOKEN_PROD: 'prod-token',
      },
      fetchImpl,
      log: vi.fn(),
      retryDelayMs: 1_000,
      sleep: ms => {
        sleeps.push(ms);
        return Promise.resolve();
      },
      version,
    }),
  ).resolves.toEqual({imageUri, notified: ['PROD'], failed: ['STAGING']});

  // Every configured target is attempted even when an earlier one fails.
  expect(stagingAttempts).toBe(3);
  expect(sleeps).toEqual([1_000, 2_000]);
});

test('notifyCloudZero does not retry 4xx responses', async () => {
  let attempts = 0;
  const fetchImpl: NotifyFetch = () => {
    attempts++;
    return Promise.resolve(errorResponse(401, 'bad token'));
  };

  await expect(
    notifyCloudZero({
      env: {
        CLOUDZERO_IMAGE_RELEASER_URL_STAGING: 'https://staging.example',
        CLOUDZERO_IMAGE_RELEASER_TOKEN_STAGING: 'staging-token',
      },
      fetchImpl,
      log: vi.fn(),
      version,
    }),
  ).resolves.toEqual({imageUri, notified: [], failed: ['STAGING']});

  expect(attempts).toBe(1);
});

test('notifyCloudZero recovers when a retry succeeds', async () => {
  let attempts = 0;
  const fetchImpl: NotifyFetch = () => {
    attempts++;
    if (attempts === 1) {
      return Promise.reject(new Error('connect timeout'));
    }
    return Promise.resolve(okResponse('{"ok":true}'));
  };

  await expect(
    notifyCloudZero({
      env: {
        CLOUDZERO_IMAGE_RELEASER_URL_STAGING: 'https://staging.example',
        CLOUDZERO_IMAGE_RELEASER_TOKEN_STAGING: 'staging-token',
      },
      fetchImpl,
      log: vi.fn(),
      sleep: () => Promise.resolve(),
      version,
    }),
  ).resolves.toEqual({imageUri, notified: ['STAGING'], failed: []});

  expect(attempts).toBe(2);
});

function okResponse(body: string) {
  return {ok: true, status: 200, text: () => Promise.resolve(body)};
}

function errorResponse(status: number, body: string) {
  return {ok: false, status, text: () => Promise.resolve(body)};
}
