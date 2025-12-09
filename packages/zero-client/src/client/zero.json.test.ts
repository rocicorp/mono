import {expect, test} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  json,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {refCountSymbol} from '../../../zql/src/ivm/view-apply-change.ts';
import {createCRUDBuilder} from '../../../zql/src/mutate/crud.ts';
import {defineMutatorsWithType} from '../../../zql/src/mutate/mutator-registry.ts';
import {defineMutatorWithType} from '../../../zql/src/mutate/mutator.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import {zeroForTest} from './test-utils.ts';

test('we can create rows with json columns and query those rows', async () => {
  const schema = createSchema({
    tables: [
      table('track')
        .columns({
          id: string(),
          title: string(),
          artists: json<string[]>(),
        })
        .primaryKey('id'),
    ],
  });

  const crud = createCRUDBuilder(schema);

  const mutators = defineMutatorsWithType<typeof schema>()({
    insertTrack: defineMutatorWithType<typeof schema>()<{
      id: string;
      title: string;
      artists: string[];
    }>(({tx, args}) => tx.mutate(crud.track.insert(args))),
  });

  const {insertTrack} = mutators;

  const z = zeroForTest({
    schema,
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
