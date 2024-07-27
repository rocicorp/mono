import {beforeEach, describe, expect, test, vi} from 'vitest';
import type {TransactionPool} from 'zero-cache/src/db/transaction-pool.js';
import {Notifier} from './notifier.js';

describe('replicator/notifier', () => {
  let notifier: Notifier;

  beforeEach(() => {
    notifier = new Notifier();
  });

  function mockTransactionPool() {
    return {
      ref: vi.fn(),
      unref: vi.fn(),
    };
  }

  test('coalesce, consumed, and cleanup', async () => {
    const sub = await notifier.addSubscription();

    const pool03 = mockTransactionPool();
    const pool04 = mockTransactionPool();
    const pool05 = mockTransactionPool();

    notifier.notifySubscribers({
      prevVersion: '03',
      newVersion: '04',
      prevSnapshotID: 'snapshot-03',
      readers: pool03 as unknown as TransactionPool,
      invalidations: {
        foo: '04',
        bar: '04',
      },
      changes: [
        {schema: 'public', table: 'foo'},
        {schema: 'public', table: 'bar'},
      ],
    });
    notifier.notifySubscribers({
      prevVersion: '04',
      newVersion: '05',
      prevSnapshotID: 'snapshot-04',
      readers: pool04 as unknown as TransactionPool,
      invalidations: {
        bar: '05',
        baz: '05',
      },
      changes: [
        {schema: 'public', table: 'food'},
        {schema: 'public', table: 'bard'},
      ],
    });

    let i = 0;
    loop: for await (const msg of sub) {
      switch (i++) {
        case 0:
          expect(msg).toEqual({
            prevVersion: '03',
            newVersion: '05',
            prevSnapshotID: 'snapshot-03',
            invalidations: {
              foo: '04',
              bar: '05',
              baz: '05',
            },
            changes: [
              {schema: 'public', table: 'foo'},
              {schema: 'public', table: 'bar'},
              {schema: 'public', table: 'food'},
              {schema: 'public', table: 'bard'},
            ],
          });
          expect(pool03.unref).not.toHaveBeenCalled();
          expect(pool04.unref).toHaveBeenCalledOnce(); // coalesced pool is released
          expect(pool05.unref).not.toHaveBeenCalled();

          notifier.notifySubscribers({
            prevVersion: '05',
            newVersion: '06',
            prevSnapshotID: 'snapshot-05',
            readers: pool05 as unknown as TransactionPool,
          });
          break;

        case 1:
          expect(msg).toEqual({
            prevVersion: '05',
            newVersion: '06',
            prevSnapshotID: 'snapshot-05',
          });
          expect(pool03.unref).toHaveBeenCalledOnce(); // consumed pool is released
          expect(pool04.unref).toHaveBeenCalledOnce();
          expect(pool05.unref).not.toHaveBeenCalled();
          break loop;
      }
    }

    expect(pool05.unref).toHaveBeenCalledOnce(); // unconsumed pool is released
  });
});
