import {beforeEach, expect, test, vi} from 'vitest';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {groupTableName} from '../../../zql/src/builder/builder.ts';
import {groupKey} from '../../../zql/src/ivm/group-by.ts';
import {refCountSymbol} from '../../../zql/src/ivm/view-apply-change.ts';
import type {
  Transaction,
  TransactionReason,
} from '../../../zql/src/mutate/custom.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {legacySchema} from '../../../zql/src/query/test/test-schemas.ts';
import {ConnectionStatus} from './connection-status.ts';
import {
  asCustomQuery,
  MockSocket,
  queryID,
  tickAFewTimes,
  zeroForTest,
} from './test-utils.ts';

beforeEach(() => {
  vi.useFakeTimers();
  vi.stubGlobal('WebSocket', MockSocket as unknown as typeof WebSocket);
  return () => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  };
});

const zql = createBuilder(legacySchema);

// A view or tx.run row.
const rc = (row: Row) => ({...row, [refCountSymbol]: 1});

function issue(id: string, closed: boolean, createdAt = 1) {
  return {id, title: id, description: '', closed, createdAt};
}

// A synced group row, as the server streams it.
function groupRow(closed: boolean, count: number) {
  return {closed, _0_key: groupKey([closed]), _0_value: count};
}

test.each(['optimistic', 'rebase'] as const)(
  'a mutator reads its own writes in a synced groupBy during %s',
  async reason => {
    // Only the open issues are counted, so a closed one moves nothing.
    const q = asCustomQuery(
      zql.issue.where('closed', false).groupBy('closed').count(),
      'openIssueCount',
      'all',
    );
    const table = groupTableName(queryID(q));
    const reads: {
      reason: TransactionReason;
      before: unknown;
      after: unknown;
    }[] = [];
    const z = zeroForTest({
      schema: legacySchema,
      mutators: {
        issue: {
          insertAndCount: async (tx: Transaction<typeof legacySchema>) => {
            const before = await tx.run(q);
            await tx.mutate.issue.insert(issue('i9', false));
            await tx.mutate.issue.insert(issue('i10', true));
            reads.push({reason: tx.reason, before, after: await tx.run(q)});
          },
        },
      },
    });
    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);
    const view = z.materialize(q);
    try {
      // Only the groups are synced, not the issues they count.
      await z.triggerPoke({
        gotQueriesPatch: [{op: 'put', hash: queryID(q)}],
        rowsPatch: [{op: 'put', tableName: table, value: groupRow(false, 7)}],
      });
      await tickAFewTimes(vi);
      expect(view.data).toEqual([rc({closed: false, count: 7})]);

      await z.mutate.issue.insertAndCount().client;
      await tickAFewTimes(vi);
      expect(view.data).toEqual([rc({closed: false, count: 8})]);

      if (reason === 'rebase') {
        // Replay the unacknowledged insert over a new server count.
        await z.triggerPoke({
          rowsPatch: [
            {op: 'put', tableName: table, value: groupRow(false, 20)},
          ],
        });
        await tickAFewTimes(vi);
        expect(view.data).toEqual([rc({closed: false, count: 21})]);
      }

      const before = reason === 'optimistic' ? 7 : 20;
      expect(reads.filter(read => read.reason === reason)).toEqual([
        {
          reason,
          before: [rc({closed: false, count: before})],
          after: [rc({closed: false, count: before + 1})],
        },
      ]);
    } finally {
      view.destroy();
      await z.close();
    }
  },
);

test('a mutator reads its new and removed values in a synced distinct', async () => {
  const q = asCustomQuery(zql.issue.distinct('closed'), 'issueStates', 'all');
  const table = groupTableName(queryID(q));
  const reads: unknown[] = [];
  const z = zeroForTest({
    schema: legacySchema,
    mutators: {
      issue: {
        closeTheLastOpen: async (tx: Transaction<typeof legacySchema>) => {
          reads.push(await tx.run(q));
          await tx.mutate.issue.insert(issue('i9', true));
          reads.push(await tx.run(q));
          await tx.mutate.issue.delete({id: 'i1'});
          reads.push(await tx.run(q));
        },
      },
    },
  });
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
  const view = z.materialize(q);
  try {
    // The open value has one issue, i1, which the client also has.
    await z.triggerPoke({
      gotQueriesPatch: [{op: 'put', hash: queryID(q)}],
      rowsPatch: [
        {op: 'put', tableName: table, value: groupRow(false, 1)},
        {op: 'put', tableName: 'issues', value: issue('i1', false)},
      ],
    });
    await tickAFewTimes(vi);
    expect(view.data).toEqual([rc({closed: false})]);

    await z.mutate.issue.closeTheLastOpen().client;
    await tickAFewTimes(vi);
    expect(reads).toEqual([
      [rc({closed: false})],
      [rc({closed: false}), rc({closed: true})],
      [rc({closed: true})],
    ]);
    expect(view.data).toEqual([rc({closed: true})]);
  } finally {
    view.destroy();
    await z.close();
  }
});

test.each(['sum', 'avg'] as const)(
  'a mutator reads its own writes in a synced groupBy().%s()',
  async fn => {
    const grouped = zql.issue.groupBy('closed');
    // Both functions give the group's value under their name.
    const times: AnyQuery =
      fn === 'sum' ? grouped.sum('createdAt') : grouped.avg('createdAt');
    const q = asCustomQuery(times, 'issueTimes', 'all');
    const table = groupTableName(queryID(q));
    const reads: unknown[] = [];
    const z = zeroForTest({
      schema: legacySchema,
      mutators: {
        issue: {
          insertAndRead: async (tx: Transaction<typeof legacySchema>) => {
            await tx.mutate.issue.insert(issue('i9', false, 7));
            reads.push(await tx.run(q));
          },
        },
      },
    });
    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);
    const view = z.materialize(q);
    try {
      // Two open issues whose times add up to 3.
      await z.triggerPoke({
        gotQueriesPatch: [{op: 'put', hash: queryID(q)}],
        rowsPatch: [
          {
            op: 'put',
            tableName: table,
            value: {
              closed: false,
              _0_key: groupKey([false]),
              _0_value: fn === 'sum' ? 3 : 1.5,
              _0_sum: 3,
              _0_count: 2,
            },
          },
        ],
      });
      await tickAFewTimes(vi);
      expect(view.data).toEqual([
        rc({closed: false, [fn]: fn === 'sum' ? 3 : 1.5}),
      ]);

      await z.mutate.issue.insertAndRead().client;
      await tickAFewTimes(vi);
      const after = [rc({closed: false, [fn]: fn === 'sum' ? 10 : 10 / 3})];
      expect(reads).toEqual([after]);
      expect(view.data).toEqual(after);
    } finally {
      view.destroy();
      await z.close();
    }
  },
);

test('an optimistic count moves the synced group an orderBy reorders', async () => {
  // The rows are ordered by owner first, and the server keys a group by its
  // values in that order: [owner, closed].
  const q = asCustomQuery(
    zql.issue.groupBy('closed', 'ownerId').count().orderBy('ownerId', 'desc'),
    'issuesByOwner',
    'all',
  );
  const table = groupTableName(queryID(q));
  const z = zeroForTest({
    schema: legacySchema,
    mutators: {
      issue: {
        addOwned: async (tx: Transaction<typeof legacySchema>) => {
          await tx.mutate.issue.insert({...issue('i9', false), ownerId: 'u1'});
        },
      },
    },
  });
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
  const view = z.materialize(q);
  try {
    await z.triggerPoke({
      gotQueriesPatch: [{op: 'put', hash: queryID(q)}],
      rowsPatch: [
        {
          op: 'put',
          tableName: table,
          value: {
            closed: false,
            ['owner_id']: 'u1',
            _0_key: groupKey(['u1', false]),
            _0_value: 2,
          },
        },
      ],
    });
    await tickAFewTimes(vi);
    expect(view.data).toEqual([rc({closed: false, ownerId: 'u1', count: 2})]);

    await z.mutate.issue.addOwned().client;
    await tickAFewTimes(vi);
    expect(view.data).toEqual([rc({closed: false, ownerId: 'u1', count: 3})]);
  } finally {
    view.destroy();
    await z.close();
  }
});

test('an edit within a group the client lacks adds no group', async () => {
  const q = asCustomQuery(zql.issue.distinct('closed'), 'issueStates', 'all');
  const z = zeroForTest({
    schema: legacySchema,
    mutators: {
      issue: {
        retitle: async (tx: Transaction<typeof legacySchema>) => {
          await tx.mutate.issue.update({id: 'i1', title: 'renamed'});
        },
      },
    },
  });
  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);
  const view = z.materialize(q);
  try {
    // i1 is synced, by another query, before the server answers this one,
    // so its group isn't on the client. Only the server can say what it is.
    await z.triggerPoke({
      rowsPatch: [{op: 'put', tableName: 'issues', value: issue('i1', false)}],
    });
    await tickAFewTimes(vi);
    expect(view.data).toEqual([]);

    await z.mutate.issue.retitle().client;
    await tickAFewTimes(vi);
    expect(view.data).toEqual([]);
  } finally {
    view.destroy();
    await z.close();
  }
});
