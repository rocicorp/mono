import {describe, expect, test} from 'vitest';
import {Subscription} from '../../types/subscription.js';
import {HashSubscriptions} from './hash-subscriptions.js';
import type {QueryInvalidationUpdate} from './invalidation-watcher.js';

describe('invalidation-watcher/hash-subscriptions', () => {
  test('add, remove, compute updates', () => {
    const hashes = new HashSubscriptions();

    const sub1 = Subscription.create<QueryInvalidationUpdate>();
    const req1 = {
      queries: {
        q1: {
          filters: [],
          hashes: ['h1', 'h2'],
        },
        q2: {
          filters: [],
          hashes: ['h2', 'h3'],
        },
      },
    };
    const sub2 = Subscription.create<QueryInvalidationUpdate>();
    const req2 = {
      queries: {
        q1: {
          filters: [],
          hashes: ['h3', 'h4'],
        },
        q2: {
          filters: [],
          hashes: ['h5', 'h6'],
        },
      },
    };
    const sub3 = Subscription.create<QueryInvalidationUpdate>();
    const req3 = {
      queries: {
        q1: {
          filters: [],
          hashes: ['h6', 'h7'],
        },
        q2: {
          filters: [],
          hashes: ['h8', 'h9'],
        },
      },
    };

    hashes.add(sub1, req1);
    hashes.add(sub2, req2);
    hashes.add(sub3, req3);

    expect(
      hashes.computeInvalidationUpdates(new Set(['h1', 'h2', 'h5', 'h6'])),
    ).toEqual(
      new Map([
        [sub1, new Set(['q1', 'q2'])],
        [sub2, new Set(['q2'])],
        [sub3, new Set(['q1'])],
      ]),
    );
    expect(
      hashes.computeInvalidationUpdate(new Set(['h1', 'h2', 'h5', 'h6']), sub1),
    ).toEqual(new Set(['q1', 'q2']));
    expect(
      hashes.computeInvalidationUpdate(new Set(['h1', 'h2', 'h5', 'h6']), sub2),
    ).toEqual(new Set(['q2']));
    expect(
      hashes.computeInvalidationUpdate(new Set(['h1', 'h2', 'h5', 'h6']), sub3),
    ).toEqual(new Set(['q1']));

    hashes.remove(sub2, req2);

    expect(
      hashes.computeInvalidationUpdates(new Set(['h1', 'h2', 'h5', 'h6'])),
    ).toEqual(
      new Map([
        [sub1, new Set(['q1', 'q2'])],
        [sub3, new Set(['q1'])],
      ]),
    );
    expect(
      hashes.computeInvalidationUpdate(new Set(['h1', 'h2', 'h5', 'h6']), sub1),
    ).toEqual(new Set(['q1', 'q2']));
    expect(
      hashes.computeInvalidationUpdate(new Set(['h1', 'h2', 'h5', 'h6']), sub3),
    ).toEqual(new Set(['q1']));
    expect(
      hashes.computeInvalidationUpdate(new Set(['h1', 'h2', 'h5', 'h6']), sub2),
    ).toEqual(new Set());

    hashes.remove(sub3, req3);

    expect(
      hashes.computeInvalidationUpdates(
        new Set(['h1', 'h2', 'h5', 'h6', 'h7', 'h8']),
      ),
    ).toEqual(new Map([[sub1, new Set(['q1', 'q2'])]]));
    expect(
      hashes.computeInvalidationUpdate(
        new Set(['h1', 'h2', 'h5', 'h6', 'h7', 'h8']),
        sub1,
      ),
    ).toEqual(new Set(['q1', 'q2']));

    hashes.remove(sub1, req1);

    expect(
      hashes.computeInvalidationUpdates(
        new Set(['h1', 'h2', 'h5', 'h6', 'h7', 'h8']),
      ),
    ).toEqual(new Map());
  });
});
