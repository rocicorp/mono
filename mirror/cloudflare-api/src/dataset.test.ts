import {describe, expect, test} from '@jest/globals';
import * as v from 'shared/src/valita.js';
import {Dataset} from './dataset.js';

const runningConnectionSeconds = new Dataset(
  'RunningConnectionSeconds',
  v.object({
    teamID: v.string(),
    appID: v.string(),
    elapsed: v.number(),
    interval: v.number(),
  }),
);

describe('dataset', () => {
  test('dataPoint', () => {
    expect(
      runningConnectionSeconds.dataPoint({
        appID: 'foo-app',
        teamID: 'bar-team',
        interval: 60,
        elapsed: 45.23,
      }),
    ).toEqual({
      blobs: ['bar-team', 'foo-app'],
      doubles: [45.23, 60],
    });

    // Order of entries in the object should not matter
    expect(
      runningConnectionSeconds.dataPoint({
        elapsed: 54.23,
        appID: 'far-app',
        interval: 59,
        teamID: 'boo-team',
      }),
    ).toEqual({
      blobs: ['boo-team', 'far-app'],
      doubles: [54.23, 59],
    });
  });

  // SQL-related functionality is tested in sql.test.ts
});
