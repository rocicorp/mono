import {expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import type {Sink} from '../../../types/streams.ts';
import {Subscription} from '../../../types/subscription.ts';
import type {BackfillMessage} from '../common/backfill-manager.ts';
import type {
  BackfillRequest,
  ChangeStreamMessage,
  MessageBackfill,
} from '../protocol/current.ts';
import {Acker, LagReporter, PostgresChangeSource} from './change-source.ts';
import type {ServerContext} from './initial-sync.ts';
import type {StreamMessage} from './logical-replication/stream.ts';
import type {
  BackupOptions,
  InternalShardConfig,
  Replica,
} from './schema/shard.ts';

/**
 * Regression test for the backfill-starvation priority inversion (a ~30 minute
 * idle gap observed in the canary logs), driving the real
 * {@link PostgresChangeSource} replication + backfill multiplexing loop (with
 * the upstream `subscribe()` and `streamBackfill()` dependencies mocked).
 *
 * Root cause: a *different* shard's lag report arrives as a standalone,
 * non-transactional logical `message`. It was misclassified as transactional,
 * so the main replication producer reserved the change-stream for it even
 * though it produces no changes. The backfill would commit/release to hand over
 * the reservation, and the main producer would then sit on the (empty)
 * reservation — starving the backfill until the next real upstream transaction.
 *
 * The fix classifies a non-transactional foreign-shard `message` as
 * non-transactional, so the main producer never reserves for it. This test
 * asserts that such a message does not interrupt a running backfill at all: the
 * backfill streams straight through it in a single, uninterrupted transaction
 * (no commit boundary, no handover). Reverting the fix — reserving for the
 * foreign message — starves the backfill and this test fails (batch 2 never
 * arrives).
 */
test('a non-transactional foreign-shard message does not interrupt a running backfill', async () => {
  const lc = createSilentLogContext();

  // A backfill message batch tagged with distinct rowValues so we can spot it
  // in the downstream change stream.
  const backfillMessage = (id: number): BackfillMessage => ({
    message: {
      tag: 'backfill',
      relation: {schema: 'public', name: 'foo', rowKey: {columns: ['id']}},
      columns: [],
      watermark: '00',
      rowValues: [[id]],
    } satisfies MessageBackfill,
    byteSize: 10,
  });

  // The mocked upstream logical replication stream. We push StreamMessages into
  // it to drive the main producer.
  const messages = Subscription.create<StreamMessage>();
  const acks: Sink<bigint> = {
    push: () => ({result: Promise.resolve('consumed')}),
  };

  // The mocked backfill stream. We push BackfillMessages into it to drive the
  // BackfillManager, one row batch at a time.
  const backfillStream = new Channel<BackfillMessage>();

  const source = new PostgresChangeSource(
    lc,
    'postgres://upstream/unused', // never connected (subscribe is mocked)
    {appID: 'zero', shardNum: 0},
    {
      slot: 'test_slot',
      publications: ['zero_pub'],
      initialSchema: {},
    } as unknown as Replica,
    {backupPath: null, backupV5: false} as unknown as BackupOptions,
    {} as ServerContext,
    0, // lagReportIntervalMs: no LagReporter
    false,
    undefined,
    {
      subscribe: () => Promise.resolve({messages, acks}),
      streamBackfill: () => backfillStream[Symbol.asyncIterator](),
    },
  );

  const backfillRequest: BackfillRequest = {
    table: {schema: 'public', name: 'foo', metadata: null},
    columns: {id: {}},
  };

  const shardConfig = {
    publications: ['zero_pub'],
  } as unknown as InternalShardConfig;

  const stream = await source.startStreamInternal(
    'test_slot',
    '00',
    shardConfig,
    [backfillRequest],
  );

  // Consume the downstream change stream in the background, recording every
  // message so we can observe backfill progress.
  const output: ChangeStreamMessage[] = [];
  void (async () => {
    for await (const change of stream.changes) {
      output.push(change);
    }
  })();

  // Flushes the microtask queue. The reserve/release/push machinery is entirely
  // promise-based (no timers or IO in this test), so draining microtasks
  // deterministically settles each step.
  const settle = async () => {
    for (let i = 0; i < 50; i++) {
      await Promise.resolve();
    }
  };

  const dataRows = () =>
    output
      .filter(c => c[0] === 'data' && c[1].tag === 'backfill')
      .flatMap(c => (c[1] as MessageBackfill).rowValues);

  // 1. Backfill streams its first batch, opening a transaction and holding the
  //    change-stream reservation.
  backfillStream.push(backfillMessage(1));
  await settle();
  expect(dataRows()).toEqual([[1]]);

  // 2. A different shard's lag report arrives on the main stream as a
  //    standalone, non-transactional logical message. The main producer must
  //    classify it as non-transactional and NOT reserve the stream.
  const foreignLagReport: StreamMessage = [
    1n,
    {
      tag: 'message',
      flags: 0,
      transactional: false,
      messageLsn: null,
      prefix: 'zero/1/lag-report/v1', // a different shard (zero/1)
      content: new Uint8Array(),
    },
  ];
  messages.push(foreignLagReport);
  await settle();

  // 3. The backfill streams its second batch. Because the foreign message never
  //    reserved the stream, the backfill was never preempted: both batches are
  //    in the same transaction.
  backfillStream.push(backfillMessage(2));
  await settle();

  // The backfill streamed straight through the foreign message, uninterrupted:
  // both batches delivered, within a single transaction (one begin, no commit
  // boundary / handover in between).
  expect(dataRows()).toEqual([[1], [2]]);
  expect(output.filter(c => c[0] === 'begin')).toHaveLength(1);
  expect(output.some(c => c[0] === 'commit')).toBe(false);

  messages.cancel();
  await source.stop();
});

/**
 * A minimal push-driven async iterable, standing in for the `streamBackfill()`
 * AsyncGenerator. `push()` enqueues a value; the consumer's `for await`
 * receives values as they arrive and otherwise waits.
 */
class Channel<T> {
  readonly #buffer: T[] = [];
  #wake: (() => void) | null = null;

  push(value: T): void {
    this.#buffer.push(value);
    const wake = this.#wake;
    this.#wake = null;
    wake?.();
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<T> {
    for (;;) {
      while (this.#buffer.length) {
        yield this.#buffer.shift() as T;
      }
      await new Promise<void>(resolve => {
        this.#wake = resolve;
      });
    }
  }
}

test('acker', () => {
  const sink = {push: vi.fn()};

  let acks = 0;

  const expectAck = (expected: bigint) => {
    expect(sink.push).toBeCalledTimes(++acks);
    expect(sink.push.mock.calls[acks - 1][0]).toBe(expected);
  };

  const expectNoAck = () => {
    expect(sink.push).toBeCalledTimes(acks);
  };

  const acker = new Acker(sink);

  acker.onChange(['status', {ack: false}, {watermark: '0a'}]);
  expectAck(10n);

  acker.onChange(['begin', {tag: 'begin'}, {commitWatermark: '0b'}]);
  acker.ack('0b');
  expectAck(11n);

  acker.onChange(['status', {ack: false}, {watermark: '0c'}]);
  expectAck(12n);

  acker.onChange(['begin', {tag: 'begin'}, {commitWatermark: '0d'}]);

  // This should be dropped because we are awaiting 0d
  acker.onChange(['status', {ack: false}, {watermark: '0e'}]);
  expectNoAck();

  // Now we are awaiting 0f
  acker.onChange(['status', {ack: true}, {watermark: '0f'}]);
  acker.ack('0d');
  expectAck(13n);

  // Still not caught up, so dropped
  acker.onChange(['status', {ack: false}, {watermark: '0g'}]);
  expectNoAck();

  // Downstream is now caught up.
  acker.ack('0f');
  expectAck(15n);

  // Now that downstream is caught up, this should respond
  acker.onChange(['status', {ack: false}, {watermark: '0h'}]);
  expectAck(17n);
});

test('lag reporter retries missing reports', async () => {
  vi.useFakeTimers();
  vi.setSystemTime(1_000);

  const dbMock = vi.fn((strings: TemplateStringsArray) => {
    if (strings.join('').includes('current_setting')) {
      return [{pgVersion: 170000}];
    }

    return [
      {
        commitTimeMs: Date.now(),
        lsn: `0/${dbMock.mock.calls.length.toString(16)}`,
      },
    ];
  });
  const db = dbMock as unknown as PostgresDB;

  const reporter = new LagReporter(
    createSilentLogContext(),
    {appID: 'test', shardNum: 0},
    db,
    10,
  );

  try {
    await expect(reporter.initiateLagReport()).resolves.toEqual({
      firstCommitTimeMs: 1_000,
      nextSendTimeMs: 1_000,
    });
    expect(dbMock).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(9);
    expect(dbMock).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(1);
    expect(dbMock).toHaveBeenCalledTimes(3);

    await vi.advanceTimersByTimeAsync(10);
    expect(dbMock).toHaveBeenCalledTimes(4);
  } finally {
    reporter.stop();
    vi.useRealTimers();
  }
});
