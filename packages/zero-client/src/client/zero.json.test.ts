import {expect, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  json,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {DefaultSchema} from '../../../zero-types/src/default-types.ts';
import {refCountSymbol} from '../../../zql/src/ivm/view-apply-change.ts';
import type {Transaction} from '../../../zql/src/mutate/custom.ts';
import {defineMutators} from '../../../zql/src/mutate/mutator-registry.ts';
import {defineMutator} from '../../../zql/src/mutate/mutator.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import {zeroForTest} from './test-utils.ts';

test('we can create rows with json columns and query those rows', async () => {
  const mutators = defineMutators({
    insertTrack: defineMutator(
      ({
        tx,
        args,
      }: {
        tx: Transaction<DefaultSchema>;
        args: {
          id: string;
          title: string;
          artists: string[];
        };
      }) => tx.mutate.track.insert(args),
    ),
  });

  const {insertTrack} = mutators;

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
    mutators,
  });

  await z.mutate(
    insertTrack({
      id: 'track-1',
      title: 'track 1',
      artists: ['artist 1', 'artist 2'],
    }),
  ).client;
  await z.mutate(
    insertTrack({
      id: 'track-2',
      title: 'track 2',
      artists: ['artist 2', 'artist 3'],
    }),
  ).client;

  const zql = createBuilder(z.schema);

  const tracks = await z.run(zql.track);

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
