import {expect, test, vi} from 'vitest';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {defineMutatorsWithType} from '../../../zql/src/mutate/mutator-registry.ts';
import {defineMutatorWithType} from '../../../zql/src/mutate/mutator.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import {zeroForTest} from './test-utils.ts';

test('Zero Junction', async () => {
  const eventSchema = table('event')
    .columns({
      id: string(),
      name: string(),
    })
    .primaryKey('id');
  const athleteSchema = table('athlete')
    .columns({
      id: string(),
      name: string(),
    })
    .primaryKey('id');
  const disciplineSchema = table('discipline')
    .columns({
      id: string(),
      name: string(),
    })
    .primaryKey('id');
  const matchupSchema = table('matchup')
    .columns({
      eventID: string(),
      athleteID: string(),
      disciplineID: string(),
    })
    .primaryKey('eventID', 'athleteID', 'disciplineID');

  const eventRelation = relationships(eventSchema, ({many}) => ({
    athletes: many(
      {sourceField: ['id'], destField: ['eventID'], destSchema: matchupSchema},
      {
        sourceField: ['athleteID'],
        destField: ['id'],
        destSchema: athleteSchema,
      },
    ),
  }));

  const schema = createSchema({
    tables: [eventSchema, athleteSchema, disciplineSchema, matchupSchema],
    relationships: [eventRelation],
  });

  const mutators = defineMutatorsWithType<typeof schema>()({
    doBatch: defineMutatorWithType<typeof schema>()(async ({tx}) => {
      await tx.mutate.event.insert({
        id: 'e1',
        name: 'Buffalo Big Board Classic',
      });
      await tx.mutate.athlete.insert({id: 'a1', name: 'Mason Ho'});
      await tx.mutate.discipline.insert({id: 'd1', name: 'Shortboard'});
      await tx.mutate.discipline.insert({id: 'd2', name: 'Supsquatch'});

      await tx.mutate.matchup.insert({
        eventID: 'e1',
        athleteID: 'a1',
        disciplineID: 'd1',
      });
      await tx.mutate.matchup.insert({
        eventID: 'e1',
        athleteID: 'a1',
        disciplineID: 'd2',
      });
    }),
  });
  const z = zeroForTest({
    schema,
    mutators,
  });
  const zql = createBuilder(schema);
  const q = zql.event.related('athletes');
  const view = z.materialize(q);
  const listener = vi.fn();
  view.addListener(listener);

  expect(listener).toHaveBeenCalledTimes(1);
  expect(view.data).toMatchInlineSnapshot(`[]`);

  await z.mutate(mutators.doBatch()).client;

  expect(listener).toHaveBeenCalledTimes(2);
  expect(view.data).toMatchInlineSnapshot(`
    [
      {
        "athletes": [
          {
            "id": "a1",
            "name": "Mason Ho",
            Symbol(rc): 2,
          },
        ],
        "id": "e1",
        "name": "Buffalo Big Board Classic",
        Symbol(rc): 1,
      },
    ]
  `);
});
