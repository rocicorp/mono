import {expect, test} from 'vitest';
import {ThreadWriteWorkerClient} from './write-worker-client.ts';

test('calls settle once the worker has exited', async () => {
  const client = new ThreadWriteWorkerClient();
  await client.stop();

  await expect(client.stop()).resolves.toBeUndefined();
  await expect(client.getSubscriptionState()).rejects.toThrow(
    'Worker exited before getSubscriptionState',
  );
});
