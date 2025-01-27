import {expect, test} from 'vitest';
import {createSchema, json, string, table} from '../mod.ts';
import {zeroForTest} from './test-utils.ts';

test('we can create rows with json columns and query those rows', async () => {
  const z = zeroForTest({
    schema: createSchema(1, {
      tables: [
        table('track')
          .columns({
            id: string(),
            title: string(),
            artists: json<string[]>(),
          })
          .primaryKey('id'),
      ],
    }),
  });

  await z.mutate.track.insert({
    id: 'track-1',
    title: 'track 1',
    artists: ['artist 1', 'artist 2'],
  });
  await z.mutate.track.insert({
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
