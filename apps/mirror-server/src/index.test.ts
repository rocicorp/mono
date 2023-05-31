import {unstable_dev} from 'wrangler';
import type {UnstableDevWorker} from 'wrangler';
import {describe, expect, it, beforeAll, afterAll} from 'vitest';

describe('Worker', () => {
  let worker: UnstableDevWorker;

  beforeAll(async () => {
    worker = await unstable_dev('src/index.ts', {
      experimental: {disableExperimentalWarning: true},
    });
  });

  afterAll(async () => {
    await worker.stop();
  });

  it('should return mirror', async () => {
    const resp = await worker.fetch(``, {method: `GET`});
    if (resp) {
      const text = await resp.text();
    }
  });
});
