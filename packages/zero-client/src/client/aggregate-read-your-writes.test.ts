import {beforeEach, expect, test, vi} from 'vitest';
import type {
  Transaction,
  TransactionReason,
} from '../../../zql/src/mutate/custom.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
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

test.each(['optimistic', 'rebase'] as const)(
  'a custom mutator reads its optimistic aggregate write during %s',
  async reason => {
    const q = asCustomQuery(
      createBuilder(legacySchema)
        .issue.where('id', 'i1')
        .related('comments', c => c.count())
        .one(),
      'issueWithCommentCount',
      'i1',
    );
    const aggregateTable = `aggregate:${queryID(q)}:comments`;
    const reads: {
      reason: TransactionReason;
      before: number | undefined;
      after: number | undefined;
    }[] = [];
    const z = zeroForTest({
      schema: legacySchema,
      mutators: {
        comment: {
          insertAndReadCount: async (
            tx: Transaction<typeof legacySchema>,
            {id}: {id: string},
          ) => {
            const before = (await tx.run(q))?.comments;
            await tx.mutate.comment.insert({
              id,
              issueId: 'i1',
              authorId: 'u1',
              text: 'new comment',
              createdAt: 1,
            });
            const after = (await tx.run(q))?.comments;
            reads.push({reason: tx.reason, before, after});
          },
        },
      },
    });
    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);
    const view = z.materialize(q);
    try {
      // Only the count is synced: none of its seven underlying comments are
      // present locally. The query must read the server's aggregate source.
      await z.triggerPoke({
        gotQueriesPatch: [{op: 'put', hash: queryID(q)}],
        rowsPatch: [
          {
            op: 'put',
            tableName: 'issues',
            value: {
              id: 'i1',
              title: 'issue',
              description: '',
              closed: false,
              owner_id: null,
              createdAt: 1,
            },
          },
          {
            op: 'put',
            tableName: aggregateTable,
            value: {issue_id: 'i1', _0_value: 7},
          },
        ],
      });
      await tickAFewTimes(vi);
      expect(view.data?.comments).toBe(7);

      await z.mutate.comment.insertAndReadCount({id: 'c1'}).client;
      await tickAFewTimes(vi);
      expect(view.data?.comments).toBe(8);

      if (reason === 'rebase') {
        // Do not acknowledge the pending insert: replay it over a new server
        // count and read the aggregate again inside the replayed transaction.
        await z.triggerPoke({
          rowsPatch: [
            {
              op: 'put',
              tableName: aggregateTable,
              value: {issue_id: 'i1', _0_value: 20},
            },
          ],
        });
        await tickAFewTimes(vi);
        expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
        expect(view.data?.comments).toBe(21);
      }

      expect(reads.filter(read => read.reason === reason)).toEqual([
        {
          reason,
          before: reason === 'optimistic' ? 7 : 20,
          after: reason === 'optimistic' ? 8 : 21,
        },
      ]);
    } finally {
      view.destroy();
      await z.close();
    }
  },
);
