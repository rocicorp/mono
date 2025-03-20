import {describe, it, expect} from 'vitest';
import {MutationTracker} from './mutation-tracker.ts';
import type {PushResponse} from '../../../zero-protocol/src/push.ts';

describe('MutationTracker', () => {
  const CLIENT_ID = 'test-client-1';

  it('tracks a mutation and resolves on success', async () => {
    const tracker = new MutationTracker(CLIENT_ID);
    const mutationPromise = tracker.trackMutation(1);

    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {},
        },
      ],
    };

    tracker.processPushResponse(response);
    const result = await mutationPromise;
    expect(result).toEqual({});
  });

  it('tracks a mutation and rejects on error', async () => {
    const tracker = new MutationTracker(CLIENT_ID);
    const mutationPromise = tracker.trackMutation(1);

    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {
            error: 'app',
            details: '',
          },
        },
      ],
    };

    tracker.processPushResponse(response);
    await expect(mutationPromise).rejects.toEqual({
      error: 'app',
      details: '',
    });
  });

  it('handles push errors', async () => {
    const tracker = new MutationTracker(CLIENT_ID);
    const mutationPromise = tracker.trackMutation(1);

    const response: PushResponse = {
      error: 'unsupported-push-version',
      mutationIDs: [{clientID: CLIENT_ID, id: 1}],
    };

    tracker.processPushResponse(response);
    await expect(mutationPromise).rejects.toEqual({
      error: 'unsupported-push-version',
      mutationIDs: [{clientID: CLIENT_ID, id: 1}],
    });
  });

  it('rejects mutation when explicitly rejected', async () => {
    const tracker = new MutationTracker(CLIENT_ID);
    const mutationPromise = tracker.trackMutation(1);

    tracker.rejectMutation(1, new Error('Failed to send'));

    await expect(mutationPromise).rejects.toThrow('Failed to send');
  });

  it('rejects mutations from other clients', () => {
    const tracker = new MutationTracker(CLIENT_ID);
    void tracker.trackMutation(1);

    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: 'other-client', id: 1},
          result: {
            error: 'app',
            details: '',
          },
        },
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: {},
        },
      ],
    };

    expect(() => tracker.processPushResponse(response)).toThrow(
      'received mutation for the wrong client',
    );
  });

  it('handles multiple concurrent mutations', async () => {
    const tracker = new MutationTracker(CLIENT_ID);
    const mutation1 = tracker.trackMutation(1);
    const mutation2 = tracker.trackMutation(2);

    const r1 = {};
    const r2 = {};
    const response: PushResponse = {
      mutations: [
        {
          id: {clientID: CLIENT_ID, id: 1},
          result: r1,
        },
        {
          id: {clientID: CLIENT_ID, id: 2},
          result: r2,
        },
      ],
    };

    tracker.processPushResponse(response);

    const [result1, result2] = await Promise.all([mutation1, mutation2]);
    expect(result1).toBe(r1);
    expect(result2).toBe(r2);
  });
});
