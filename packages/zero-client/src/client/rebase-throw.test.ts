import {beforeEach, expect, test, vi} from 'vitest';
import type {InsertValue} from '../../../zql/src/mutate/crud.ts';
import type {Transaction} from '../../../zql/src/mutate/custom.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import {legacySchema} from '../../../zql/src/query/test/test-schemas.ts';
import {ConnectionStatus} from './connection-status.ts';
import {
  asCustomQuery,
  MockSocket,
  tickAFewTimes,
  zeroForTest,
} from './test-utils.ts';

type Schema = typeof legacySchema;
type MutatorTx = Transaction<Schema>;
type Issue = InsertValue<typeof legacySchema.tables.issue>;

const issue = (id: string): Issue => ({
  id,
  title: `title ${id}`,
  description: '',
  closed: false,
  createdAt: 42,
});

beforeEach(() => {
  vi.useFakeTimers();
  vi.stubGlobal('WebSocket', MockSocket as unknown as typeof WebSocket);
  return () => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  };
});

/**
 * A custom mutator body runs again on every rebase, so a guard that passed when
 * the user made the mutation can fail when it is replayed onto a newer server
 * snapshot. That race is normal, and this is the contract it has to meet.
 *
 * Before the fix in db/rebase.ts the throw escaped the rebase, which failed the
 * poke and disconnected the client, and the mutation stayed pending so the next
 * poke failed the same way. `addUnlessFull` writes before it throws, which is
 * the other half: each replayed mutation runs against its own fork of Zero's
 * IVM sources, so a mutator that throws takes its writes to them down with it.
 * Without the fork the mutations replayed after it read rows that were never
 * committed.
 */
test('a mutator whose guard fails on rebase does not break the client', async () => {
  let replays = 0;
  const idsSeenOnReplay: string[][] = [];

  const z = zeroForTest({
    schema: legacySchema,
    mutators: {
      issue: {
        // Writes, then throws once the server snapshot pushes it over a limit.
        addUnlessFull: async (tx: MutatorTx, args: Issue) => {
          if (tx.reason === 'rebase') {
            replays++;
          }
          await tx.mutate.issue.insert(args);
          const all = await tx.run(tx.query.issue);
          if (all.length > 2) {
            throw new Error('too many issues');
          }
        },
        // Reads through ZQL, so it sees whatever branch it was handed.
        addAndRecord: async (tx: MutatorTx, args: Issue) => {
          const all = await tx.run(tx.query.issue.orderBy('id', 'asc'));
          if (tx.reason === 'rebase') {
            idsSeenOnReplay.push(all.map(r => r.id));
          }
          await tx.mutate.issue.insert(args);
        },
      },
    } as const,
  });

  await z.triggerConnected();
  await z.waitForConnectionStatus(ConnectionStatus.Connected);

  const zql = createBuilder(legacySchema);
  const q = asCustomQuery(zql.issue.orderBy('id', 'asc'), 'allIssues', []);
  const view = z.materialize(q);

  try {
    await z.triggerGotQueriesPatch(q);
    await tickAFewTimes(vi);

    // Both pass optimistically against an empty store.
    await z.mutate.issue.addUnlessFull(issue('a')).client;
    await z.mutate.issue.addAndRecord(issue('c')).client;
    await tickAFewTimes(vi);
    expect(view.data.map(r => r.id)).toEqual(['a', 'c']);

    // Two rows arrive without acking either mutation, so both are replayed and
    // the first one's limit is now exceeded.
    await z.triggerPoke({
      rowsPatch: [
        {op: 'put', tableName: 'issues', value: issue('x')},
        {op: 'put', tableName: 'issues', value: issue('y')},
      ],
    });
    await tickAFewTimes(vi);

    // The contract the old code broke: the throw must not take the poke, and
    // with it the connection, down.
    expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
    expect(replays).toBe(1);

    // The abandoned mutation's row is not visible to the mutation replayed
    // after it. Without the per-mutation fork this is ['a', 'x', 'y'].
    expect(idsSeenOnReplay).toEqual([['x', 'y']]);

    // It leaves nothing behind in the view either, while the mutation replayed
    // after it still applies.
    expect(view.data.map(r => r.id)).toEqual(['c', 'x', 'y']);

    // Abandoning the prediction does not abandon the mutation. It is still
    // pending and still the server's to decide on, so this poke, which also
    // does not ack it, replays it again.
    await z.triggerPoke({
      rowsPatch: [{op: 'put', tableName: 'issues', value: issue('z')}],
    });
    await tickAFewTimes(vi);
    expect(replays).toBe(2);
    expect(idsSeenOnReplay).toEqual([
      ['x', 'y'],
      ['x', 'y', 'z'],
    ]);
    expect(view.data.map(r => r.id)).toEqual(['c', 'x', 'y', 'z']);
    expect(z.connectionStatus).toBe(ConnectionStatus.Connected);

    // Once the server acks them they retire: no cycle that repeats forever.
    await z.triggerPoke({
      lastMutationIDChanges: {[z.clientID]: 2},
      rowsPatch: [{op: 'put', tableName: 'issues', value: issue('w')}],
    });
    await tickAFewTimes(vi);
    expect(replays).toBe(2);
    expect(z.connectionStatus).toBe(ConnectionStatus.Connected);
  } finally {
    view.destroy();
    await z.close();
  }
});
