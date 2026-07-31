import {LogContext} from '@rocicorp/logger';
import {resolver, type Resolver} from '@rocicorp/resolver';
import fc from 'fast-check';
import {afterEach, describe, expect, test, vi} from 'vitest';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {Downstream, WatermarkedChange} from './change-streamer.ts';
import * as ErrorType from './error-type-enum.ts';
import {Forwarder} from './forwarder.ts';
import {
  SQLiteChangeLogCatchup,
  SQLiteChangeLogBarrierBacklogError,
  SQLiteChangeLogBarrierTimeoutError,
  type SQLiteChangeLogCatchupOptions,
  type SQLiteChangeLogCatchupReader,
  type SQLiteChangeLogCleanupGuard,
} from './sqlite-change-log-catchup.ts';
import type {CatchupPlan} from './sqlite-change-log-reader.ts';
import {Subscriber, type SubscriberOptions} from './subscriber.ts';

const coordinators: SQLiteChangeLogCatchup[] = [];

const FUZZ_SEED_WATERMARK = '0001';
type CatchupFuzzScenario = {
  readonly priorWidths: number[];
  readonly requestedPosition: number;
  readonly sqlitePosition: number;
  readonly registrationPhase: 'between' | 'after-begin' | 'after-data';
  readonly inFlightCommits: boolean;
  readonly inFlightWidth: number;
  readonly futureWidths: number[];
  readonly sentinelWidth: number;
  readonly batchSize: number;
  readonly wakeup: 'commit' | 'early-commit' | 'poll';
};

const catchupFuzzScenario: fc.Arbitrary<CatchupFuzzScenario> = fc.record({
  priorWidths: fc.array(fc.integer({min: 0, max: 5}), {maxLength: 5}),
  requestedPosition: fc.nat({max: 100}),
  sqlitePosition: fc.nat({max: 100}),
  registrationPhase: fc.constantFrom(
    'between' as const,
    'after-begin' as const,
    'after-data' as const,
  ),
  inFlightCommits: fc.boolean(),
  inFlightWidth: fc.integer({min: 0, max: 5}),
  futureWidths: fc.array(fc.integer({min: 0, max: 5}), {maxLength: 5}),
  sentinelWidth: fc.integer({min: 0, max: 5}),
  batchSize: fc.integer({min: 1, max: 5}),
  wakeup: fc.constantFrom(
    'commit' as const,
    'early-commit' as const,
    'poll' as const,
  ),
});

describe('SQLiteChangeLogCatchup', () => {
  afterEach(() => {
    for (const coordinator of coordinators.splice(0)) {
      coordinator.close();
    }
  });

  test('registers before pinning the head and deduplicates live overlap', async () => {
    const fixture = createFixture();
    fixture.reader.head = '04';
    fixture.reader.entries.push(...transaction('04'));
    fixture.reader.boundaries.add('04');

    const {subscriber, output} = createSubscriber('01');
    await fixture.coordinator.catchup(subscriber, () => '06');

    // This replay reaches the Forwarder after registration but before SQLite
    // reaches the required head. It is both in the backlog and, once applied,
    // in the pinned catchup range.
    forward(fixture.forwarder, transaction('06'));
    fixture.reader.entries.push(...transaction('06'));
    fixture.reader.boundaries.add('06');
    fixture.reader.head = '06';

    expect(await takeMarkers(output, 7)).toEqual([
      'status',
      '04:begin',
      '04:insert',
      '04:commit',
      '06:begin',
      '06:insert',
      '06:commit',
    ]);
    expect(fixture.reader.reads).toEqual([
      {from: '01', through: '06', batchSize: 2},
    ]);
  });

  test('waits for a mid-transaction commit before choosing the required head', async () => {
    const fixture = createFixture();
    fixture.reader.entries.push(...transaction('04'));
    fixture.reader.boundaries.add('04');
    fixture.reader.head = '04';

    fixture.forwarder.forward(entry('06', 'begin'));
    const completion = resolver<string>();
    const {subscriber, output} = createSubscriber('04');
    await fixture.coordinator.catchup(subscriber, () => completion.promise);

    fixture.forwarder.forward(entry('06', 'insert'));
    fixture.forwarder.forward(entry('06', 'commit'));
    fixture.reader.entries.push(...transaction('06'));
    fixture.reader.boundaries.add('06');
    fixture.reader.head = '06';
    completion.resolve('06');

    forward(fixture.forwarder, transaction('08'));
    expect(await takeMarkers(output, 7)).toEqual([
      'status',
      '06:begin',
      '06:insert',
      '06:commit',
      '08:begin',
      '08:insert',
      '08:commit',
    ]);
  });

  test('uses the prior committed head when the registration transaction rolls back', async () => {
    const fixture = createFixture();
    fixture.reader.entries.push(...transaction('04'));
    fixture.reader.boundaries.add('04');
    fixture.reader.head = '04';

    fixture.forwarder.forward(entry('06', 'begin'));
    const completion = resolver<string>();
    const {subscriber, output} = createSubscriber('01');
    await fixture.coordinator.catchup(subscriber, () => completion.promise);
    fixture.forwarder.forward(entry('06', 'rollback'));
    completion.resolve('04');

    forward(fixture.forwarder, transaction('08'));
    expect(await takeMarkers(output, 7)).toEqual([
      'status',
      '04:begin',
      '04:insert',
      '04:commit',
      '08:begin',
      '08:insert',
      '08:commit',
    ]);
  });

  test('buffers commits that arrive across several catchup batches', async () => {
    const fixture = createFixture();
    fixture.reader.entries.push(...transaction('04'), ...transaction('06'));
    fixture.reader.boundaries.add('04');
    fixture.reader.boundaries.add('06');
    fixture.reader.head = '06';
    const releaseRead = resolver<void>();
    fixture.reader.beforeRead = releaseRead.promise;

    const {subscriber, output} = createSubscriber('01');
    await fixture.coordinator.catchup(subscriber, () => '06');
    forward(fixture.forwarder, transaction('08'));
    forward(fixture.forwarder, transaction('0a'));
    releaseRead.resolve();

    expect(await takeMarkers(output, 13)).toEqual([
      'status',
      ...transactionMarkers('04'),
      ...transactionMarkers('06'),
      ...transactionMarkers('08'),
      ...transactionMarkers('0a'),
    ]);
  });

  test('accepts a subscriber ahead of SQLite and filters replayed live commits', async () => {
    const fixture = createFixture();
    fixture.reader.entries.push(...transaction('04'));
    fixture.reader.boundaries.add('04');
    fixture.reader.head = '04';

    const {subscriber, output} = createSubscriber('06');
    await fixture.coordinator.catchup(subscriber, () => '04');
    forward(fixture.forwarder, transaction('06'));
    forward(fixture.forwarder, transaction('08'));

    expect(await takeMarkers(output, 4)).toEqual([
      'status',
      ...transactionMarkers('08'),
    ]);
  });

  test('closes the restore-skew gap while SQLite replays to the forwarded head', async () => {
    const fixture = createFixture();
    fixture.reader.entries.push(...transaction('04'));
    fixture.reader.boundaries.add('04');
    fixture.reader.head = '04';

    // The change-streamer already forwarded through 06, but the restored
    // canonical replica is only at 04. A subscriber reconnecting at 06 must
    // wait for SQLite to replay 04..06 without receiving 06 twice.
    const {subscriber, output} = createSubscriber('06');
    await fixture.coordinator.catchup(subscriber, () => '06');
    forward(fixture.forwarder, transaction('06'));
    fixture.reader.entries.push(...transaction('06'));
    fixture.reader.boundaries.add('06');
    fixture.reader.head = '06';
    forward(fixture.forwarder, transaction('08'));

    expect(await takeMarkers(output, 4)).toEqual([
      'status',
      ...transactionMarkers('08'),
    ]);
  });

  test('fuzzes gap-free catchup-to-live interleavings', async () => {
    // Reuse one fixture per batch size. Constructing a Forwarder per fast-check
    // run would register hundreds of process-lifetime metric callbacks.
    const fixtures = new Map(
      [1, 2, 3, 4, 5].map(
        batchSize => [batchSize, createFixture({batchSize})] as const,
      ),
    );

    try {
      await fc.assert(
        fc.asyncProperty(catchupFuzzScenario, scenario =>
          runCatchupFuzzScenario(fixtures.get(scenario.batchSize), scenario),
        ),
        {numRuns: 500},
      );
    } finally {
      for (const {coordinator} of fixtures.values()) {
        coordinator.close();
      }
    }
  });

  test("releases the barrier on the change-log writer's commit", async () => {
    // A poll interval well beyond the test timeout: only the commit
    // notification can release this barrier, so passing proves it is not
    // polling for the head.
    const fixture = createFixture({
      barrierTimeoutMs: 60_000,
      barrierPollIntervalMs: 60_000,
    });
    fixture.reader.entries.push(...transaction('04'));
    fixture.reader.boundaries.add('04');
    fixture.reader.head = '04';

    const plan = vi.spyOn(fixture.reader, 'plan');
    const {subscriber, output} = createSubscriber('01');
    await fixture.coordinator.catchup(subscriber, () => '06');
    await vi.waitFor(() => expect(plan).toHaveBeenCalledOnce());

    fixture.reader.entries.push(...transaction('06'));
    fixture.reader.boundaries.add('06');
    fixture.reader.head = '06';
    fixture.coordinator.onChangeLogCommit('06');

    expect(await takeMarkers(output, 7)).toEqual([
      'status',
      ...transactionMarkers('04'),
      ...transactionMarkers('06'),
    ]);
  });

  test('ignores change-log commits below the required head', async () => {
    const fixture = createFixture({
      barrierTimeoutMs: 60_000,
      barrierPollIntervalMs: 60_000,
    });
    fixture.reader.head = '04';

    const plan = vi.spyOn(fixture.reader, 'plan');
    const {subscriber, output} = createSubscriber('01');
    await fixture.coordinator.catchup(subscriber, () => '06');
    await vi.waitFor(() => expect(plan).toHaveBeenCalledOnce());

    // The writer is still behind the required head. Waking on every commit
    // would re-read the log at the writer's commit rate for no reason.
    fixture.coordinator.onChangeLogCommit('04');
    await sleep(20);
    expect(plan).toHaveBeenCalledOnce();
    expect(output.size()).toBe(0);

    fixture.reader.entries.push(...transaction('06'));
    fixture.reader.boundaries.add('06');
    fixture.reader.head = '06';
    fixture.coordinator.onChangeLogCommit('06');

    expect(await takeMarkers(output, 4)).toEqual([
      'status',
      ...transactionMarkers('06'),
    ]);
  });

  test('re-reads the replica rather than trusting the ACK', async () => {
    const fixture = createFixture({
      barrierTimeoutMs: 60_000,
      barrierPollIntervalMs: 60_000,
    });
    fixture.reader.head = '04';

    const plan = vi.spyOn(fixture.reader, 'plan');
    const {subscriber, output} = createSubscriber('01');
    const backlogFull = subscriber.whenBacklogFull();
    const backlogThen = vi.spyOn(backlogFull.promise, 'then');
    const cancelBacklogWait = vi.spyOn(backlogFull, 'cancel');
    vi.spyOn(subscriber, 'whenBacklogFull').mockReturnValue(backlogFull);
    await fixture.coordinator.catchup(subscriber, () => '06');
    await vi.waitFor(() => expect(plan).toHaveBeenCalledOnce());

    // An ACK that the reader cannot yet corroborate wakes the barrier but does
    // not release it: plan() decides what is readable.
    fixture.coordinator.onChangeLogCommit('06');
    await vi.waitFor(() => expect(plan).toHaveBeenCalledTimes(2));
    expect(output.size()).toBe(0);

    fixture.reader.entries.push(...transaction('06'));
    fixture.reader.boundaries.add('06');
    fixture.reader.head = '06';
    fixture.coordinator.onChangeLogCommit('06');

    expect(await takeMarkers(output, 4)).toEqual([
      'status',
      ...transactionMarkers('06'),
    ]);
    expect(backlogThen).toHaveBeenCalledOnce();
    expect(cancelBacklogWait).toHaveBeenCalledOnce();
  });

  test('maps too-old plans to WatermarkTooOld', async () => {
    const fixture = createFixture();
    fixture.reader.min = '04';
    fixture.reader.head = '06';
    fixture.reader.boundaries.add('04');
    const {output, subscriber} = createSubscriber('01');
    await fixture.coordinator.catchup(subscriber, () => '06');
    expect(await output.dequeue()).toEqual([
      'error',
      {
        type: ErrorType.WatermarkTooOld,
        message: 'earliest supported watermark is 04 (requested 01)',
      },
    ]);
  });

  test('gives up the barrier once the subscriber backlog fills', async () => {
    // Long enough that only the backlog can end this wait. Bytes, not time,
    // are the real bound; the deadline is only a backstop for an idle shard.
    const fixture = createFixture({
      barrierTimeoutMs: 60_000,
      barrierPollIntervalMs: 60_000,
    });
    fixture.reader.head = '01';

    const {subscriber, done, output} = createSubscriber('01', {
      backlogHighWaterBytes: 1,
    });
    const fail = vi.spyOn(subscriber, 'fail');
    await fixture.coordinator.catchup(subscriber, () => '06');

    // Past the high water mark this subscriber's send() no longer resolves,
    // so it holds up the flushes that would advance the replica it waits on.
    forward(fixture.forwarder, transaction('06'));

    await done;
    // A clean end, same as a barrier timeout: reconnecting is cheaper than
    // holding replication while the backlog grows.
    expect(fail).not.toHaveBeenCalled();
    expect(output.size()).toBe(0);
    expect(fixture.logSink.messages).toContainEqual([
      'warn',
      expect.anything(),
      [
        expect.stringContaining('to retry SQLite catchup'),
        expect.objectContaining({
          name: SQLiteChangeLogBarrierBacklogError.name,
        }),
      ],
    ]);
  });

  test('ends only the selected subscription, cleanly, on barrier timeout', async () => {
    const fixture = createFixture({barrierTimeoutMs: 5});
    fixture.reader.head = '01';
    const {subscriber, done, output} = createSubscriber('01');
    const fail = vi.spyOn(subscriber, 'fail');
    await fixture.coordinator.catchup(subscriber, () => '06');

    await done;
    // A clean cancel rather than an ['error', ...] message. IncrementalSyncer
    // treats any error message as terminal and restores from litestream; a
    // clean end backs off and re-subscribes, which is what "the replica is not
    // caught up yet" warrants.
    expect(fail).not.toHaveBeenCalled();
    expect(output.size()).toBe(0);
    expect(fixture.logSink.messages).toContainEqual([
      'warn',
      expect.anything(),
      [
        expect.stringContaining('to retry SQLite catchup'),
        expect.objectContaining({
          name: SQLiteChangeLogBarrierTimeoutError.name,
        }),
      ],
    ]);
  });

  test('waits out a transaction longer than the barrier timeout', async () => {
    const fixture = createFixture({barrierTimeoutMs: 5});
    fixture.reader.entries.push(...transaction('04'));
    fixture.reader.boundaries.add('04');
    fixture.reader.head = '04';

    const completion = resolver<string>();
    const then = vi.spyOn(completion.promise, 'then');
    const {subscriber, output} = createSubscriber('01');
    const fail = vi.spyOn(subscriber, 'fail');

    await fixture.coordinator.catchup(subscriber, () => completion.promise);

    // Well past barrierTimeoutMs with the transaction still in flight. The
    // barrier bounds replica lag, so this wait must not consume its budget.
    await sleep(50);
    expect(fail).not.toHaveBeenCalled();

    fixture.reader.entries.push(...transaction('06'));
    fixture.reader.boundaries.add('06');
    fixture.reader.head = '06';
    completion.resolve('06');

    // The barrier still has its full budget once the required head is known.
    expect(await takeMarkers(output, 7)).toEqual([
      'status',
      ...transactionMarkers('04'),
      ...transactionMarkers('06'),
    ]);
    expect(then).toHaveBeenCalledOnce();
  });

  test('aborting during the required-head wait releases the subscriber', async () => {
    const fixture = createFixture();
    const completion = resolver<string>();
    const {subscriber, done} = createSubscriber('01');
    const fail = vi.spyOn(subscriber, 'fail');

    await fixture.coordinator.catchup(subscriber, () => completion.promise);
    fixture.coordinator.remove(subscriber);

    await done;
    expect(fail).not.toHaveBeenCalled();
    expect(
      fixture.logSink.messages.filter(([level]) => level === 'error'),
    ).toEqual([]);
  });

  test('fails closed on reader errors after registration', async () => {
    const fixture = createFixture();
    fixture.reader.head = '06';
    fixture.reader.boundaries.add('01');
    fixture.reader.readError = new Error('broken SQLite read');
    const {subscriber, done, output} = createSubscriber('01');
    await fixture.coordinator.catchup(subscriber, () => '06');

    // fail() ends the subscription without a downstream ['error', ...], so the
    // subscriber reconnects instead of restoring a replica. The error reaches
    // operators through the log below, not through the subscriber.
    await done;
    expect(output.size()).toBe(0);
    expect(fixture.logSink.messages).toContainEqual([
      'error',
      expect.anything(),
      [
        expect.stringContaining('error while catching up subscriber'),
        expect.objectContaining({message: 'broken SQLite read'}),
      ],
    ]);
  });

  test('cancellation and shutdown release catchup resources', async () => {
    const fixture = createFixture();
    fixture.reader.head = '01';
    const first = createSubscriber('01');
    await fixture.coordinator.catchup(first.subscriber, () => '06');
    fixture.coordinator.remove(first.subscriber);

    const second = createSubscriber('01');
    await fixture.coordinator.catchup(second.subscriber, () => '06');
    fixture.coordinator.close();

    expect(fixture.reader.closed).toBe(true);
    expect(fixture.forwarder.getAcks()).toEqual(new Set());
  });

  test('registers under the cleanup guard before exposing the subscriber ACK', async () => {
    const guardGate = resolver<void>();
    const cleanupGuard: SQLiteChangeLogCleanupGuard = {
      async runWhilePurgeBlocked(register) {
        await guardGate.promise;
        return register();
      },
    };
    const fixture = createFixture({cleanupGuard});
    fixture.reader.head = '01';
    fixture.reader.boundaries.add('01');
    const {subscriber} = createSubscriber('01');
    const registering = fixture.coordinator.catchup(subscriber, () => '01');

    expect(fixture.forwarder.getAcks()).toEqual(new Set());
    guardGate.resolve();
    await registering;
    expect(fixture.forwarder.getAcks()).toEqual(new Set(['01']));
  });
});

function createFixture(
  opts: {
    batchSize?: number | undefined;
    barrierTimeoutMs?: number | undefined;
    barrierPollIntervalMs?: number | undefined;
    cleanupGuard?: SQLiteChangeLogCleanupGuard | undefined;
    now?: SQLiteChangeLogCatchupOptions['now'];
    sleep?: SQLiteChangeLogCatchupOptions['sleep'];
  } = {},
) {
  const logSink = new TestLogSink();
  const lc = new LogContext('debug', undefined, logSink);
  const reader = new TestReader();
  const forwarder = new Forwarder(lc);
  const coordinator = new SQLiteChangeLogCatchup(lc, forwarder, reader, {
    batchSize: opts.batchSize ?? 2,
    barrierTimeoutMs: opts.barrierTimeoutMs ?? 1_000,
    barrierPollIntervalMs: opts.barrierPollIntervalMs ?? 1,
    cleanupGuard: opts.cleanupGuard,
    now: opts.now,
    sleep: opts.sleep,
  });
  coordinators.push(coordinator);
  return {coordinator, forwarder, logSink, reader};
}

class TestReader implements SQLiteChangeLogCatchupReader {
  readonly boundaries = new Set(['01']);
  readonly entries: WatermarkedChange[] = [];
  readonly reads: {from: string; through: string; batchSize: number}[] = [];
  min = '01';
  head = '01';
  beforeRead: Promise<void> | undefined;
  readError: Error | undefined;
  closed = false;

  plan(fromWatermark: string): CatchupPlan {
    if (fromWatermark > this.head) {
      return {kind: 'ahead', headWatermark: this.head};
    }
    if (fromWatermark < this.min || !this.boundaries.has(fromWatermark)) {
      return {
        kind: 'too-old',
        minWatermark: this.min,
        headWatermark: this.head,
      };
    }
    return {
      kind: 'range',
      minWatermark: this.min,
      headWatermark: this.head,
    };
  }

  async *read(
    fromWatermark: string,
    throughWatermark: string,
    batchSize: number,
    signal?: AbortSignal,
  ): AsyncIterable<readonly WatermarkedChange[]> {
    this.reads.push({
      from: fromWatermark,
      through: throughWatermark,
      batchSize,
    });
    await this.beforeRead;
    if (signal?.aborted) {
      throw new AbortError();
    }
    if (this.readError) {
      throw this.readError;
    }
    const selected = this.entries.filter(
      ([watermark]) =>
        watermark > fromWatermark && watermark <= throughWatermark,
    );
    for (let i = 0; i < selected.length; i += batchSize) {
      if (signal?.aborted) {
        throw new AbortError();
      }
      yield selected.slice(i, i + batchSize);
    }
  }

  close(): void {
    this.closed = true;
  }
}

function createSubscriber(watermark: string, options: SubscriberOptions = {}) {
  const downstream = Subscription.create<string>();
  const subscriber = new Subscriber(
    5,
    `subscriber-${watermark}`,
    watermark,
    downstream,
    () => ({tag: 'status'}),
    options,
  );
  const {done, output} = drainToQueue(downstream);
  return {done, subscriber, output};
}

function drainToQueue(source: Source<string>): {
  done: Promise<void>;
  output: Queue<Downstream>;
} {
  const queue = new Queue<Downstream>();
  const done = (async () => {
    for await (const json of source) {
      queue.enqueue(BigIntJSON.parse(json) as Downstream);
    }
  })();
  return {done, output: queue};
}

function entry(
  watermark: string,
  tag: 'begin' | 'insert' | 'commit' | 'rollback',
): WatermarkedChange {
  const downstreamTag = tag === 'insert' ? 'data' : tag;
  return [
    watermark,
    tag,
    JSON.stringify([downstreamTag, {tag, marker: `${watermark}:${tag}`}]),
  ];
}

function transaction(watermark: string): WatermarkedChange[] {
  return [
    entry(watermark, 'begin'),
    entry(watermark, 'insert'),
    entry(watermark, 'commit'),
  ];
}

function transactionMarkers(watermark: string) {
  return [`${watermark}:begin`, `${watermark}:insert`, `${watermark}:commit`];
}

type FuzzTransaction = {
  readonly changes: WatermarkedChange[];
  readonly markers: string[];
  readonly watermark: string;
};

function fuzzTransaction(index: number, width: number): FuzzTransaction {
  const watermark = String((index + 1) * 2).padStart(4, '0');
  const inserts = Array.from({length: width}, (_, i) => {
    const marker = `${watermark}:insert-${i}`;
    return {
      change: [
        watermark,
        'insert',
        JSON.stringify(['data', {tag: 'insert', marker}]),
      ] satisfies WatermarkedChange,
      marker,
    };
  });
  return {
    watermark,
    changes: [
      entry(watermark, 'begin'),
      ...inserts.map(({change}) => change),
      entry(watermark, 'commit'),
    ],
    markers: [
      `${watermark}:begin`,
      ...inserts.map(({marker}) => marker),
      `${watermark}:commit`,
    ],
  };
}

async function runCatchupFuzzScenario(
  fixture: ReturnType<typeof createFixture> | undefined,
  scenario: CatchupFuzzScenario,
): Promise<void> {
  if (!fixture) {
    throw new Error(`missing fixture for batch size ${scenario.batchSize}`);
  }
  const {coordinator, forwarder, reader} = fixture;
  expect(forwarder.getAcks()).toEqual(new Set());
  resetFuzzReader(reader);

  let nextIndex = 0;
  const prior = scenario.priorWidths.map(width =>
    fuzzTransaction(nextIndex++, width),
  );
  for (const tx of prior) {
    forward(forwarder, tx.changes);
  }

  const sqlitePosition =
    scenario.sqlitePosition % (scenario.priorWidths.length + 1);
  applyFuzzTransactions(reader, prior.slice(0, sqlitePosition));

  const requestedPosition =
    scenario.requestedPosition % (scenario.priorWidths.length + 1);
  const requestedWatermark =
    requestedPosition === 0
      ? FUZZ_SEED_WATERMARK
      : (prior.at(requestedPosition - 1)?.watermark ?? FUZZ_SEED_WATERMARK);

  let requiredHead: string | Promise<string> =
    prior.at(-1)?.watermark ?? FUZZ_SEED_WATERMARK;
  let inFlight:
    | {
        readonly completion: Resolver<string>;
        readonly forwardedEntries: number;
        readonly tx: FuzzTransaction;
      }
    | undefined;
  if (scenario.registrationPhase !== 'between') {
    const tx = fuzzTransaction(nextIndex++, scenario.inFlightWidth);
    const forwardedEntries =
      scenario.registrationPhase === 'after-begin' ? 1 : tx.changes.length - 1;
    forward(forwarder, tx.changes.slice(0, forwardedEntries));
    const completion = resolver<string>();
    requiredHead = completion.promise;
    inFlight = {completion, forwardedEntries, tx};
  }

  const {done, output, subscriber} = createSubscriber(requestedWatermark);
  try {
    await coordinator.catchup(subscriber, () => requiredHead);

    if (inFlight) {
      const {completion, forwardedEntries, tx} = inFlight;
      if (scenario.inFlightCommits) {
        forward(forwarder, tx.changes.slice(forwardedEntries));
        completion.resolve(tx.watermark);
      } else {
        forward(
          forwarder,
          tx.changes.slice(forwardedEntries, tx.changes.length - 1),
        );
        forwarder.forward(entry(tx.watermark, 'rollback'));
        completion.resolve(prior.at(-1)?.watermark ?? FUZZ_SEED_WATERMARK);
      }
    }

    const future = scenario.futureWidths.map(width =>
      fuzzTransaction(nextIndex++, width),
    );
    for (const tx of future) {
      forward(forwarder, tx.changes);
    }

    const committed = [
      ...prior,
      ...(inFlight && scenario.inFlightCommits ? [inFlight.tx] : []),
      ...future,
    ];
    const appliedHead = committed.at(-1)?.watermark ?? FUZZ_SEED_WATERMARK;
    if (scenario.wakeup === 'early-commit') {
      coordinator.onChangeLogCommit(appliedHead);
      // Let an early notification race a plan() that still observes the old
      // head. The poll backstop must preserve correctness if it is spent.
      await Promise.resolve();
    }
    applyFuzzTransactions(
      reader,
      committed.filter(tx => !reader.boundaries.has(tx.watermark)),
    );
    if (scenario.wakeup === 'commit') {
      coordinator.onChangeLogCommit(appliedHead);
    }

    // This commit is deliberately live-only. Receiving it proves catchup made
    // the transition and provides an ordering sentinel after every overlap.
    const sentinel = fuzzTransaction(nextIndex, scenario.sentinelWidth);
    forward(forwarder, sentinel.changes);

    const expected = [
      'status',
      ...committed
        .filter(tx => tx.watermark > requestedWatermark)
        .flatMap(tx => tx.markers),
      ...sentinel.markers,
    ];
    expect(await takeMarkers(output, expected.length, 250)).toEqual(expected);
    await vi.waitFor(() => expect(subscriber.numPending).toBe(0), {
      timeout: 250,
    });
    expect(subscriber.acked).toBe(sentinel.watermark);
    expect(output.size()).toBe(0);
  } finally {
    coordinator.remove(subscriber);
    await done;
    expect(forwarder.getAcks()).toEqual(new Set());
  }
}

function resetFuzzReader(reader: TestReader): void {
  reader.boundaries.clear();
  reader.boundaries.add(FUZZ_SEED_WATERMARK);
  reader.entries.splice(0);
  reader.reads.splice(0);
  reader.min = FUZZ_SEED_WATERMARK;
  reader.head = FUZZ_SEED_WATERMARK;
  reader.beforeRead = undefined;
  reader.readError = undefined;
}

function applyFuzzTransactions(
  reader: TestReader,
  transactions: readonly FuzzTransaction[],
): void {
  for (const tx of transactions) {
    reader.entries.push(...tx.changes);
    reader.boundaries.add(tx.watermark);
    reader.head = tx.watermark;
  }
}

function forward(forwarder: Forwarder, changes: WatermarkedChange[]) {
  for (const change of changes) {
    forwarder.forward(change);
  }
}

async function takeMarkers(
  output: Queue<Downstream>,
  count: number,
  timeoutMs?: number,
) {
  const markers: string[] = [];
  for (let i = 0; i < count; i++) {
    const timeout = [
      'error',
      {type: ErrorType.Unknown, message: 'timed out waiting for fuzz output'},
    ] satisfies Downstream;
    const downstream =
      timeoutMs === undefined
        ? await output.dequeue()
        : await output.dequeue(timeout, timeoutMs);
    if (downstream === timeout) {
      throw new Error(timeout[1].message);
    }
    if (downstream[0] === 'error') {
      throw new Error(`unexpected downstream error: ${downstream[1].message}`);
    }
    const message = downstream[1];
    markers.push(
      message.tag === 'status'
        ? 'status'
        : String((message as unknown as {marker: string}).marker),
    );
  }
  return markers;
}
