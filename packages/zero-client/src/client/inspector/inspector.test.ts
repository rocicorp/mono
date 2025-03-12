import {beforeEach, expect, test, vi} from 'vitest';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {schema} from '../../../../zql/src/query/test/test-schemas.ts';
import {nanoid} from '../../util/nanoid.ts';
import {MockSocket, zeroForTest} from '../test-utils.ts';

const rafMock = vi.fn<typeof requestAnimationFrame>();

async function nextRaf() {
  expect(rafMock).toHaveBeenCalled();
  const f = rafMock.mock.calls[0][0];
  rafMock.mock.calls.shift();
  f(1);
  await Promise.resolve(1);
}

beforeEach(() => {
  vi.spyOn(globalThis, 'WebSocket').mockImplementation(
    () => new MockSocket('ws://localhost:1234') as unknown as WebSocket,
  );
  vi.spyOn(globalThis, 'requestAnimationFrame').mockImplementation(rafMock);
  return () => {
    vi.restoreAllMocks();
  };
});

test('basics', async () => {
  const z = zeroForTest();
  const inspector = await z.inspect();

  expect(inspector.client).toEqual({
    clientGroup: {
      id: await z.clientGroupID,
    },
    id: z.clientID,
  });
  expect(inspector.clientGroup).toEqual({
    id: await z.clientGroupID,
  });

  await z.close();
});

test('basics 2 clients', async () => {
  const userID = nanoid();
  const z1 = zeroForTest({userID, kvStore: 'idb'});
  const z2 = zeroForTest({userID, kvStore: 'idb'});

  const inspector = await z1.inspect();

  expect(await inspector.clients()).toEqual([
    {
      clientGroup: {
        id: await z1.clientGroupID,
      },
      id: z1.clientID,
    },
    {
      clientGroup: {
        id: await z2.clientGroupID,
      },
      id: z2.clientID,
    },
  ]);

  await z1.close();
  await z2.close();
});

test('queries', async () => {
  const userID = nanoid();
  const z1 = zeroForTest({userID, schema, kvStore: 'idb'});
  const z2 = zeroForTest({userID, schema, kvStore: 'idb'});
  await z1.triggerConnected();
  await z2.triggerConnected();

  await z1.triggerPoke(null, '1', {
    desiredQueriesPatches: {
      [z1.clientID]: [
        {
          op: 'put',
          hash: 'hash1',
          ast: {
            table: 'issues',
          },
          ttl: 1000,
        },
      ],
    },
  });

  await z2.triggerPoke(null, '1', {
    desiredQueriesPatches: {
      [z2.clientID]: [
        {
          op: 'put',
          hash: 'hash2',
          ast: {
            table: 'users',
          },
          ttl: 2000,
        },
      ],
    },
  });

  await nextRaf();
  await nextRaf();

  const inspector = await z1.inspect();
  expect(await inspector.clients()).toEqual([
    {
      clientGroup: {
        id: await z1.clientGroupID,
      },
      id: z1.clientID,
    },
    {
      clientGroup: {
        id: await z2.clientGroupID,
      },
      id: z2.clientID,
    },
  ]);
  expect(await inspector.clientsWithQueries()).toEqual([
    {
      clientGroup: {
        id: await z1.clientGroupID,
      },
      id: z1.clientID,
    },
  ]);
  expect(await inspector.client.queries()).toEqual([
    {
      ast: {
        table: 'issue',
      },
      got: false,
      id: 'hash1',
    },
  ]);

  await z1.triggerPoke('1', '2', {
    gotQueriesPatch: [
      {
        hash: 'hash1',
        op: 'put',
        ast: {
          table: 'issues',
        },
        ttl: 2000,
      },
    ],
  });

  await nextRaf();

  expect(await inspector.client.queries()).toEqual([
    {
      ast: {
        table: 'issue',
      },
      got: true,
      id: 'hash1',
    },
  ]);

  await z1.close();
  await z2.close();
});

test('sql getter', async () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'or',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: '1'},
        },
        {
          type: 'simple',
          op: '!=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: '2'},
        },
      ],
    },
    alias: undefined,
    limit: undefined,
    orderBy: undefined,
    related: undefined,
    schema: undefined,
    start: undefined,
  };
  const z1 = zeroForTest({schema});
  await z1.triggerConnected();
  await z1.triggerPoke(null, '1', {
    desiredQueriesPatches: {
      [z1.clientID]: [
        {
          op: 'put',
          hash: 'hash1',
          ast: {
            ...ast,
            table: 'issues',
          },
          ttl: 1000,
        },
      ],
    },
  });

  await nextRaf();

  const inspector = await z1.inspect();
  const queries = await inspector.client.queries();
  expect(queries).toEqual([
    {
      ast,
      got: false,
      id: 'hash1',
    },
  ]);
  expect(queries[0].sql).toBe(
    `SELECT * FROM "issue" WHERE ("id" = $1 OR "id" != $2)`,
  );
});
