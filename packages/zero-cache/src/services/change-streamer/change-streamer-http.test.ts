import {describe, expect, test} from 'vitest';
import {getSubscriberContext} from './change-streamer-http.ts';
import {PROTOCOL_VERSION, type BackfillDeclaration} from './change-streamer.ts';

describe('getSubscriberContext', () => {
  function req(params: Record<string, string>) {
    const search = new URLSearchParams({
      id: 'sub',
      taskID: 'task',
      replicaVersion: 'abc',
      watermark: '123',
      ...params,
    });
    return {
      url: `/replication/v${PROTOCOL_VERSION}/changes?${search.toString()}`,
      headers: {},
    };
  }

  test('backfills are absent by default', () => {
    expect(getSubscriberContext(req({})).backfills).toBe(undefined);
  });

  test('an empty backfills parameter is treated as absent', () => {
    expect(getSubscriberContext(req({backfills: ''})).backfills).toBe(
      undefined,
    );
    expect(getSubscriberContext(req({backfills: '[]'})).backfills).toEqual([]);
  });

  test('backfill declarations round trip', () => {
    const backfills: BackfillDeclaration[] = [
      {
        schema: 'public',
        table: 'issue',
        columns: ['description'],
        mark: ['1234', `it's`, 'héllo 中文'],
        markWatermark: '0a',
        runID: 'run-abc',
      },
      {
        schema: 'other',
        table: 'comment',
        columns: ['body', 'author'],
        mark: null,
        markWatermark: null,
        runID: null,
      },
    ];
    expect(
      getSubscriberContext(req({backfills: JSON.stringify(backfills)}))
        .backfills,
    ).toEqual(backfills);
  });

  test('a malformed declaration is rejected', () => {
    expect(() =>
      getSubscriberContext(req({backfills: '[{"schema":"public"}]'})),
    ).toThrow();
    expect(() => getSubscriberContext(req({backfills: 'not json'}))).toThrow();
  });
});
