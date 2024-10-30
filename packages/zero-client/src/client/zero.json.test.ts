import {expect, test} from 'vitest';
import {zeroForTest} from './test-utils.js';

test('we can create rows with json columns and query those rows', async () => {
  const z = zeroForTest({
    schema: {
      version: 1,
      tables: {
        track: {
          columns: {
            id: {type: 'string'},
            title: {type: 'string'},
            artists: {type: 'json'},
          },
          primaryKey: ['id'],
          tableName: 'track',
          relationships: {},
        },
      },
    },
  });

  await z.mutate.track.create({
    id: 'track-1',
    title: 'track 1',
    artists: ['artist 1', 'artist 2'],
  });
  await z.mutate.track.create({
    id: 'track-2',
    title: 'track 2',
    artists: ['artist 2', 'artist 3'],
  });

  const tracks = z.query.track.run();

  expect(tracks).toEqual([
    {id: 'track-1', title: 'track 1', artists: ['artist 1', 'artist 2']},
    {id: 'track-2', title: 'track 2', artists: ['artist 2', 'artist 3']},
  ]);
});
