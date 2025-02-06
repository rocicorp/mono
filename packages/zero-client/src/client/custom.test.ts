import {beforeEach, describe, expect, expectTypeOf, test} from 'vitest';
import {schema} from '../../../zql/src/query/test/test-schemas.ts';
import {
  TransactionImpl,
  type CustomMutatorDefs,
  type MakeCustomMutatorInterfaces,
  type Transaction,
} from './custom.ts';
import {zeroForTest} from './test-utils.ts';
import {nanoid} from '../util/nanoid.ts';
import {createDb} from './test/create-db.ts';
import {IVMSourceRepo} from './ivm-source-repo.ts';
import type {WriteTransaction} from './replicache-types.ts';
import {must} from '../../../shared/src/must.ts';
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
      readonly setTitle: (args: {id: string; title: string}) => void;
      readonly setProps: (args: {
        id: string;
        title: string;
        status: 'closed' | 'open';
        assignee: string;
      }) => void;
    };
    readonly nonTableNamespace: {
      readonly doThing: (args: {arg1: string; arg2: number}) => void;
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
        create: async tx => {
          await tx.mutate.issue.insert({
            id: nanoid(),
            title: '',
            closed: false,
            ownerId: '',
            description: '',
          });
        },
      },
      customNamespace: {
        clown: async (tx, id: string) => {
          await tx.mutate.issue.update({id, title: '🤡'});
        },
      },
    },
  });

  await z.mutate.issue.insert({
    id: '1',
    title: 'foo',
    closed: false,
    ownerId: '',
    description: '',
  });

  let issues = z.query.issue.run();
  expect(issues[0].title).toEqual('foo');

  await z.mutate.issue.setTitle({id: '1', title: 'bar'});
  issues = z.query.issue.run();
  expect(issues[0].title).toEqual('bar');

  await z.mutate.customNamespace.clown('1');
  issues = z.query.issue.run();
  expect(issues[0].title).toEqual('🤡');

  await z.mutate.issue.create();
  issues = z.query.issue.run();
  expect(issues.length).toEqual(2);

  await z.mutate.issue.deleteTwoIssues({id1: issues[0].id, id2: issues[1].id});
  issues = z.query.issue.run();
  expect(issues.length).toEqual(0);
});

test('custom mutators can query the local store during an optimistic mutation', async () => {
  const z = zeroForTest({
    schema,
    mutators: {
      issue: {
        closeAll: async tx => {
          await Promise.all(
            tx.query.issue
              .run()
              .map(issue =>
                tx.mutate.issue.update({id: issue.id, closed: true}),
              ),
          );
        },
      },
    },
  });

  await Promise.all(
    Array.from({length: 10}, async (_, i) => {
      await z.mutate.issue.insert({
        id: i.toString().padStart(3, '0'),
        title: `issue ${i}`,
        closed: false,
        description: '',
        ownerId: '',
      });
    }),
  );
  let issues = z.query.issue.where('closed', false).run();
  expect(issues.length).toEqual(10);

  await z.mutate.issue.closeAll();

  issues = z.query.issue.where('closed', false).run();
  expect(issues.length).toEqual(0);
});

describe('rebasing custom mutators', () => {
  let repo: IVMSourceRepo;
  beforeEach(async () => {
    const {dagStore, syncHash} = await createDb([], 42);
    repo = new IVMSourceRepo(schema.tables);
    await repo.advanceSyncHead(dagStore, syncHash, []);
  });

  test('mutations write to the rebase branch', async () => {
    const tx1 = new TransactionImpl(
      {
        reason: 'rebase',
        has: () => false,
        set: () => {},
      } as unknown as WriteTransaction,
      schema,
      repo,
    ) as unknown as Transaction<Schema>;

    await tx1.mutate.issue.insert({
      closed: false,
      description: '',
      id: '1',
      ownerId: '',
      title: 'foo',
    });

    expect([
      ...must(repo.rebase.getSource('issue'))
        .connect([['id', 'asc']])
        .fetch({}),
    ]).toMatchInlineSnapshot(`
      [
        {
          "relationships": {},
          "row": {
            "closed": false,
            "description": "",
            "id": "1",
            "ownerId": "",
            "title": "foo",
          },
        },
      ]
    `);
  });

  test('mutations can read their own writes', async () => {
    const tx1 = new TransactionImpl(
      {
        reason: 'rebase',
        has: () => false,
        set: () => {},
      } as unknown as WriteTransaction,
      schema,
      repo,
    ) as unknown as Transaction<Schema>;

    await tx1.mutate.issue.insert({
      closed: false,
      description: '',
      id: '1',
      ownerId: '',
      title: 'foo',
    });

    expect(tx1.query.issue.run()).toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "description": "",
          "id": "1",
          "ownerId": "",
          "title": "foo",
        },
      ]
    `);
  });

  test('later mutations can read writes done by earlier mutations', async () => {
    const tx1 = new TransactionImpl(
      {
        reason: 'rebase',
        has: () => false,
        set: () => {},
      } as unknown as WriteTransaction,
      schema,
      repo,
    ) as unknown as Transaction<Schema>;

    await tx1.mutate.issue.insert({
      closed: false,
      description: '',
      id: '1',
      ownerId: '',
      title: 'foo',
    });

    const tx2 = new TransactionImpl(
      {
        reason: 'rebase',
        has: () => false,
        set: () => {},
      } as unknown as WriteTransaction,
      schema,
      repo,
    ) as unknown as Transaction<Schema>;

    expect(tx2.query.issue.run()).toMatchInlineSnapshot(`
      [
        {
          "closed": false,
          "description": "",
          "id": "1",
          "ownerId": "",
          "title": "foo",
        },
      ]
    `);
  });
});
