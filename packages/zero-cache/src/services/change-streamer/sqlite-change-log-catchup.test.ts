import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {afterEach, describe, expect, test, vi} from 'vitest';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {Downstream, WatermarkedChange} from './change-streamer.ts';
import * as ErrorType from './error-type-enum.ts';
import {Forwarder} from './forwarder.ts';
import {AutoResetSignal} from './schema/tables.ts';
import {
  SQLiteChangeLogCatchup,
  SQLiteChangeLogBarrierTimeoutError,
  type SQLiteChangeLogCatchupReader,
  type SQLiteChangeLogCleanupGuard,
} from './sqlite-change-log-catchup.ts';
import type {CatchupPlan} from './sqlite-change-log-reader.ts';
import {Subscriber} from './subscriber.ts';

const coordinators: SQLiteChangeLogCatchup[] = [];

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
    await fixture.coordinator.catchup(subscriber, 'serving', () => '06');

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
    await fixture.coordinator.catchup(
      subscriber,
      'serving',
      () => completion.promise,
    );

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
    await fixture.coordinator.catchup(
      subscriber,
      'serving',
      () => completion.promise,
    );
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
    await fixture.coordinator.catchup(subscriber, 'serving', () => '06');
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
    await fixture.coordinator.catchup(subscriber, 'serving', () => '04');
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
    await fixture.coordinator.catchup(subscriber, 'serving', () => '06');
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

  test('maps too-old plans to serving and backup policies', async () => {
    const serving = createFixture();
    serving.reader.min = '04';
    serving.reader.head = '06';
    serving.reader.boundaries.add('04');
    const servingSub = createSubscriber('01');
    await serving.coordinator.catchup(
      servingSub.subscriber,
      'serving',
      () => '06',
    );
    expect(await servingSub.output.dequeue()).toEqual([
      'error',
      {
        type: ErrorType.WatermarkTooOld,
        message: 'earliest supported watermark is 04 (requested 01)',
      },
    ]);
    expect(serving.onFatal).not.toHaveBeenCalled();

    const backup = createFixture();
    backup.reader.min = '04';
    backup.reader.head = '06';
    backup.reader.boundaries.add('04');
    const backupSub = createSubscriber('01');
    await backup.coordinator.catchup(
      backupSub.subscriber,
      'backup',
      () => '06',
    );
    await backupSub.done;
    await vi.waitFor(() => expect(backup.onFatal).toHaveBeenCalledOnce());
    expect(backup.onFatal.mock.calls[0][0]).toBeInstanceOf(AutoResetSignal);
  });

  test('fails only the selected subscription on barrier timeout', async () => {
    const fixture = createFixture({barrierTimeoutMs: 5});
    fixture.reader.head = '01';
    const {subscriber, done} = createSubscriber('01');
    await fixture.coordinator.catchup(subscriber, 'serving', () => '06');

    await done;
    expect(fixture.logSink.messages).toContainEqual([
      'error',
      expect.anything(),
      [
        expect.stringContaining('error while catching up subscriber'),
        expect.objectContaining({
          name: SQLiteChangeLogBarrierTimeoutError.name,
        }),
      ],
    ]);
    expect(fixture.onFatal).not.toHaveBeenCalled();
  });

  test('fails closed on reader errors after registration', async () => {
    const fixture = createFixture();
    fixture.reader.head = '06';
    fixture.reader.boundaries.add('01');
    fixture.reader.readError = new Error('broken SQLite read');
    const {subscriber, done} = createSubscriber('01');
    await fixture.coordinator.catchup(subscriber, 'serving', () => '06');

    await done;
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
    await fixture.coordinator.catchup(first.subscriber, 'serving', () => '06');
    fixture.coordinator.remove(first.subscriber);

    const second = createSubscriber('01');
    await fixture.coordinator.catchup(second.subscriber, 'serving', () => '06');
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
    const registering = fixture.coordinator.catchup(
      subscriber,
      'serving',
      () => '01',
    );

    expect(fixture.forwarder.getAcks()).toEqual(new Set());
    guardGate.resolve();
    await registering;
    expect(fixture.forwarder.getAcks()).toEqual(new Set(['01']));
  });
});

function createFixture(
  opts: {
    barrierTimeoutMs?: number | undefined;
    cleanupGuard?: SQLiteChangeLogCleanupGuard | undefined;
  } = {},
) {
  const logSink = new TestLogSink();
  const lc = new LogContext('debug', undefined, logSink);
  const reader = new TestReader();
  const forwarder = new Forwarder(lc);
  const onFatal = vi.fn<(error: AutoResetSignal) => Promise<void>>(() =>
    Promise.resolve(),
  );
  const coordinator = new SQLiteChangeLogCatchup(lc, forwarder, reader, {
    batchSize: 2,
    barrierTimeoutMs: opts.barrierTimeoutMs ?? 1_000,
    barrierPollIntervalMs: 1,
    cleanupGuard: opts.cleanupGuard,
    onFatal,
  });
  coordinators.push(coordinator);
  return {coordinator, forwarder, logSink, onFatal, reader};
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

function createSubscriber(watermark: string) {
  const downstream = Subscription.create<string>();
  const subscriber = new Subscriber(
    5,
    `subscriber-${watermark}`,
    watermark,
    downstream,
    () => ({tag: 'status'}),
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

function forward(forwarder: Forwarder, changes: WatermarkedChange[]) {
  for (const change of changes) {
    forwarder.forward(change);
  }
}

async function takeMarkers(output: Queue<Downstream>, count: number) {
  const markers: string[] = [];
  for (let i = 0; i < count; i++) {
    const downstream = await output.dequeue();
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
