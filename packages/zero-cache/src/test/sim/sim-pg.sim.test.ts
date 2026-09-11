import {LogContext} from '@rocicorp/logger';
import fc from 'fast-check';
import {describe, expect, test} from 'vitest';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {ChangeStream} from '../../services/change-source/change-source.ts';
import type {ChangeStreamMessage} from '../../services/change-source/protocol/current/downstream.ts';
import {ChangeProcessor} from '../../services/replicator/change-processor.ts';
import {initReplicationState} from '../../services/replicator/schema/replication-state.ts';
import type {Subscription} from '../../types/subscription.ts';
import {diffReplica, replicaStateVersion} from './replica.ts';
import {SimPG, TERMINATED_BY_TAKEOVER} from './sim-pg.ts';
import {Trace} from './trace.ts';
import {applyWorkloadOp, workloadTxArb, type WorkloadOp} from './workload.ts';

// No fake timers: nothing here schedules one, so a real macrotask turn
// settles every promise chain.
const settle = () => new Promise<void>(resolve => setImmediate(resolve));

function newTrace() {
  return new Trace({now: () => 0, runDirs: []});
}

function commit(pg: SimPG, ops: readonly WorkloadOp[], gap = 1) {
  const tx = pg.begin();
  for (const op of ops) {
    applyWorkloadOp(tx, op);
  }
  return pg.commit(tx, gap);
}

function insertTx(pg: SimPG, id: number) {
  const tx = pg.begin();
  const table = tx.tableAt(0) ?? tx.createTable(['text']);
  tx.insert(table.name, {id, c2: `v${id}`});
  return pg.commit(tx, 1);
}

/** Collects a stream's messages until it ends, and how it ended. */
function collect(stream: ChangeStream) {
  const received: ChangeStreamMessage[] = [];
  let ended: 'open' | 'done' | Error = 'open';
  void (async () => {
    try {
      for await (const msg of stream.changes) {
        received.push(msg);
      }
      ended = 'done';
    } catch (e) {
      ended = e as Error;
    }
  })();
  return {
    received,
    commits: () =>
      received.flatMap(m => (m[0] === 'commit' ? [m[2].watermark] : [])),
    ended: () => ended,
  };
}

describe('sim/sim-pg', () => {
  test('a start below the confirmed flush moves forward, silently', async () => {
    const pg = new SimPG(newTrace());
    const w = [1, 2, 3].map(id => insertTx(pg, id)?.watermark);

    const first = pg.startStream(pg.replicaVersion);
    const firstOut = collect(first);
    pg.deliver(3);
    await settle();
    expect(firstOut.commits()).toEqual(w);
    first.acks.push(['status', {tag: 'commit'}, {watermark: w[1] as string}]);
    expect(pg.confirmedFlush).toBe(w[1]);

    // Nothing tells the consumer that its start moved.
    const second = pg.startStream(pg.replicaVersion);
    const secondOut = collect(second);
    pg.deliver(10);
    await settle();
    expect(secondOut.commits()).toEqual([w[2]]);
    expect(secondOut.ended()).toBe('open');
    expect(pg.takeViolations()).toEqual([]);
  });

  test('a waiting holder that is taken over fails at once', async () => {
    const pg = new SimPG(newTrace());
    insertTx(pg, 1);
    const old = collect(pg.startStream(pg.replicaVersion));
    pg.deliver(1);
    await settle();
    expect(old.commits()).toHaveLength(1);

    pg.startStream(pg.replicaVersion);
    await settle();
    expect(String(old.ended())).toContain(TERMINATED_BY_TAKEOVER);
  });

  test('a back-pressured holder fails only once it drains, dropping what was queued', async () => {
    const pg = new SimPG(newTrace());
    for (let id = 1; id <= 4; id++) {
      insertTx(pg, id);
    }
    const stream = pg.startStream(pg.replicaVersion);
    const queued = () =>
      (stream.changes as Subscription<ChangeStreamMessage>).queued;
    pg.deliver(4);
    // Nothing has consumed: the wire window holds six messages, and the rest
    // of the burst waits on the socket.
    await settle();
    expect(queued()).toBe(6);

    pg.startStream(pg.replicaVersion);
    await settle();
    // Still queued: the old holder has not read since the takeover.
    expect(queued()).toBe(6);

    const iterator = stream.changes[Symbol.asyncIterator]();
    const taken: unknown[] = [];
    let error: unknown;
    try {
      for (;;) {
        const next = await iterator.next();
        if (next.done) {
          break;
        }
        taken.push(next.value);
      }
    } catch (e) {
      error = e;
    }
    // It got one message, which made room in the window; the read that
    // followed found the error, and failing dropped the rest.
    expect(taken.length).toBeLessThan(6);
    expect(String(error)).toContain(TERMINATED_BY_TAKEOVER);
  });

  test('ACKs of a minor, or beyond the head, are violations', () => {
    const pg = new SimPG(newTrace());
    const head = insertTx(pg, 1)?.watermark as string;
    const stream = pg.startStream(pg.replicaVersion);
    stream.acks.push(['status', {tag: 'commit'}, {watermark: `${head}.01`}]);
    stream.acks.push(['status', {tag: 'commit'}, {watermark: `${head}z`}]);
    expect(pg.takeViolations()).toHaveLength(2);
    expect(pg.confirmedFlush).toBe(pg.replicaVersion);
  });

  type Step =
    | {
        readonly kind: 'commit';
        readonly ops: readonly WorkloadOp[];
        readonly gap: number;
      }
    | {readonly kind: 'deliver'; readonly n: number}
    | {readonly kind: 'keepalive'}
    | {readonly kind: 'ack'}
    | {readonly kind: 'disconnect'; readonly partial: number | undefined};

  const stepArb: fc.Arbitrary<Step> = fc.oneof(
    {
      weight: 4,
      arbitrary: fc.record({
        kind: fc.constant('commit' as const),
        ops: workloadTxArb,
        gap: fc.integer({min: 1, max: 40}),
      }),
    },
    {
      weight: 3,
      arbitrary: fc.record({
        kind: fc.constant('deliver' as const),
        n: fc.integer({min: 1, max: 4}),
      }),
    },
    {weight: 1, arbitrary: fc.constant({kind: 'keepalive' as const})},
    {weight: 1, arbitrary: fc.constant({kind: 'ack' as const})},
    {
      weight: 1,
      arbitrary: fc.record({
        kind: fc.constant('disconnect' as const),
        partial: fc.option(fc.integer({min: 1, max: 4}), {nil: undefined}),
      }),
    },
  );

  test('the plain source replicates any workload into one replica, contiguously, across disconnects', async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(stepArb, {minLength: 1, maxLength: 40}),
        async steps => {
          const lc = new LogContext('error', {}, {log: () => {}});
          const pg = new SimPG(newTrace());
          const replica = new Database(lc, ':memory:');
          try {
            initReplicationState(replica, ['zero_data'], pg.replicaVersion);
            // An aborted ChangeProcessor drops everything after, so, as the
            // write worker does, an abort replaces it.
            const newProcessor = () =>
              new ChangeProcessor(
                new StatementRunner(replica),
                'serving',
                (_lc, err) => {
                  throw err;
                },
              );
            let processor = newProcessor();
            const applied: string[] = [];
            const errors: unknown[] = [];
            let stream: ChangeStream | undefined;

            const connect = () => {
              const current = pg.startStream(replicaStateVersion(replica));
              stream = current;
              void (async () => {
                try {
                  for await (const msg of current.changes) {
                    if (msg[0] === 'status' || msg[0] === 'control') {
                      continue;
                    }
                    processor.processMessage(lc, msg);
                    if (msg[0] === 'commit') {
                      applied.push(msg[2].watermark);
                    }
                  }
                } catch (e) {
                  if (
                    !String(e).includes('connection to the upstream was lost')
                  ) {
                    errors.push(e);
                  }
                  processor.abort(lc);
                  processor = newProcessor();
                } finally {
                  if (stream === current) {
                    stream = undefined;
                  }
                }
              })();
            };

            const check = (at: string) => {
              expect(errors, at).toEqual([]);
              expect(pg.takeViolations(), at).toEqual([]);
              // Contiguous: exactly the upstream commits, in order.
              expect(applied, at).toEqual(
                pg.commits.slice(1, applied.length + 1).map(c => c.watermark),
              );
              const version = replicaStateVersion(replica);
              expect(version, at).toBe(applied.at(-1) ?? pg.replicaVersion);
              expect(
                diffReplica(replica, pg.commitAt(version).state),
                at,
              ).toEqual([]);
            };

            for (const [i, step] of steps.entries()) {
              if (!stream) {
                connect();
              }
              switch (step.kind) {
                case 'commit':
                  commit(pg, step.ops, step.gap);
                  break;
                case 'deliver':
                  pg.deliver(step.n);
                  break;
                case 'keepalive':
                  pg.keepalive();
                  break;
                case 'ack':
                  stream?.acks.push([
                    'status',
                    {tag: 'commit'},
                    {watermark: replicaStateVersion(replica)},
                  ]);
                  break;
                case 'disconnect':
                  pg.disconnect(step.partial);
                  break;
              }
              await settle();
              check(`step ${i} (${step.kind})`);
            }

            // Heal: reconnect and deliver everything.
            for (
              let round = 0;
              round < 3 && applied.length < pg.commits.length - 1;
              round++
            ) {
              if (!stream) {
                connect();
              }
              pg.deliver(pg.commits.length);
              await settle();
              await settle();
              check(`heal round ${round}`);
            }
            expect(replicaStateVersion(replica)).toBe(pg.head.watermark);
          } finally {
            replica.close();
          }
        },
      ),
      {numRuns: 200},
    );
  });
});
