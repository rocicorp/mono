import {expect, expectTypeOf, test} from 'vitest';
import {schema} from '../../../zql/src/query/test/test-schemas.ts';
import {
  type CustomMutatorDefs,
  type MakeCustomMutatorInterfaces,
} from './custom.ts';
import {zeroForTest} from './test-utils.ts';
import type {InsertValue} from '../../../zql/src/mutate/custom.ts';

type Schema = typeof schema;

test('argument types are preserved on the generated mutator interface', () => {
  const mutators = {
    issue: {
      setTitle: (tx, {id, title}: {id: string; title: string}) =>
        tx.mutate.issue.update({id, title}),
      setProps: (
        tx,
        {
          id,
          title,
          status,
          assignee,
        }: {
          id: string;
          title: string;
          status: 'open' | 'closed';
          assignee: string;
        },
      ) =>
        tx.mutate.issue.update({
          id,
          title,
          closed: status === 'closed',
          ownerId: assignee,
        }),
    },
    nonTableNamespace: {
      doThing: (_tx, _a: {arg1: string; arg2: number}) => {
        throw new Error('not implemented');
      },
    },
  } satisfies CustomMutatorDefs<Schema>;

  type MutatorsInterface = MakeCustomMutatorInterfaces<Schema, typeof mutators>;
  expectTypeOf<MutatorsInterface>().toEqualTypeOf<{
    readonly issue: {
      readonly setTitle: (args: {id: string; title: string}) => Promise<void>;
      readonly setProps: (args: {
        id: string;
        title: string;
        status: 'closed' | 'open';
        assignee: string;
      }) => Promise<void>;
    };
    readonly nonTableNamespace: {
      readonly doThing: (args: {arg1: string; arg2: number}) => Promise<void>;
    };
  }>();
});

test('cannot support non-namespace custom mutators', () => {
  ({
    // @ts-expect-error - all mutators must be in a namespace
    setTitle: (_tx, _a: {id: string; title: string}) => {
      throw new Error('not implemented');
    },
  }) satisfies CustomMutatorDefs<Schema>;
});

test('custom mutators write to the local store', async () => {
  const z = zeroForTest({
    logLevel: 'debug',
    schema,
    mutators: {
      issue: {
        setTitle: async (tx, {id, title}: {id: string; title: string}) => {
          await tx.mutate.issue.update({id, title});
        },
        deleteTwoIssues: async (tx, {id1, id2}: {id1: string; id2: string}) => {
          await Promise.all([
            tx.mutate.issue.delete({id: id1}),
            tx.mutate.issue.delete({id: id2}),
          ]);
        },
        create: async (tx, args: InsertValue<typeof schema.tables.issue>) => {
          await tx.mutate.issue.insert(args);
        },
      },
      customNamespace: {
        clown: async (tx, id: string) => {
          await tx.mutate.issue.update({id, title: '🤡'});
        },
      },
    } as const satisfies CustomMutatorDefs<Schema>,
  });

  await z.mutate.issue.create({
    id: '1',
    title: 'foo',
    closed: false,
    ownerId: '',
    description: '',
  });

  let issues = await z.query.issue.run();
  expect(issues[0].title).toEqual('foo');

  await z.mutate.issue.setTitle({id: '1', title: 'bar'});
  issues = await z.query.issue.run();
  expect(issues[0].title).toEqual('bar');

  await z.mutate.customNamespace.clown('1');
  issues = await z.query.issue.run();
  expect(issues[0].title).toEqual('🤡');

  await z.mutate.issue.create({
    id: '2',
    title: 'foo',
    closed: false,
    ownerId: '',
    description: '',
  });
  issues = await z.query.issue.run();
  expect(issues.length).toEqual(2);

  await z.mutate.issue.deleteTwoIssues({id1: issues[0].id, id2: issues[1].id});
  issues = await z.query.issue.run();
  expect(issues.length).toEqual(0);
});
