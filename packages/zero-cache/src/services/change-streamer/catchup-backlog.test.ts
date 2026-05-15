import {resolver} from '@rocicorp/resolver';
import {describe, expect, test} from 'vitest';
import {CatchupBacklog} from './catchup-backlog.ts';

describe('change-streamer/CatchupBacklog', () => {
  test('resolves an enqueue only after the entry is consumed', async () => {
    const backlog = new CatchupBacklog<number>();
    let consumed = false;

    const enqueued = backlog.enqueue(1).then(() => {
      consumed = true;
    });

    await Promise.resolve();
    expect(consumed).toBe(false);

    await backlog.flushWith(entry => {
      expect(entry).toBe(1);
    });
    await enqueued;

    expect(consumed).toBe(true);
  });

  test('includes entries enqueued while a flush is running', async () => {
    const backlog = new CatchupBacklog<number>();
    const first = backlog.enqueue(1);
    const secondConsumed = resolver<void>();
    const seen: number[] = [];

    const flush = backlog.flushWith(entry => {
      seen.push(entry);
      if (entry === 1) {
        void backlog.enqueue(2).then(() => secondConsumed.resolve());
      }
    });

    await flush;
    await first;
    await secondConsumed.promise;

    expect(seen).toEqual([1, 2]);
  });

  test('rejects all pending enqueue receipts when flushing fails', async () => {
    const backlog = new CatchupBacklog<number>();
    const error = new Error('downstream failed');
    const first = backlog.enqueue(1).catch(err => err);
    const second = backlog.enqueue(2).catch(err => err);

    await expect(
      backlog.flushWith(() => {
        throw error;
      }),
    ).rejects.toBe(error);

    await expect(first).resolves.toBe(error);
    await expect(second).resolves.toBe(error);
  });
});
