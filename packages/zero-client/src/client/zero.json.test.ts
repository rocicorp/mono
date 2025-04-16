import {expect, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  json,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {refCountSymbol} from '../../../zql/src/ivm/view-apply-change.ts';
import {zeroForTest} from './test-utils.ts';

test('we can create rows with json columns and query those rows', async () => {
  const z = zeroForTest({
    schema: createSchema({
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

  const tracks = await z.query.track;

  expect(tracks).toEqual([
    {
      id: 'track-1',
      title: 'track 1',
      artists: ['artist 1', 'artist 2'],
      [refCountSymbol]: 1,
    },
    {
      id: 'track-2',
      title: 'track 2',
      artists: ['artist 2', 'artist 3'],
      [refCountSymbol]: 1,
    },
  ]);
});
