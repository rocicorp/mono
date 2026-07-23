import {afterEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {DbFile} from '../../test/lite.ts';
import type {ShardID} from '../../types/shards.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import type {SubscriberContext} from './change-streamer.ts';
import {
  isSQLiteChangeLogReadSelected,
  SQLiteChangeLogReadSourceRouter,
  type ChangeLogState,
  type SQLiteChangeLogReadDecision,
  type SQLiteChangeLogReadSource,
  type SQLiteChangeLogReadState,
} from './sqlite-change-log-read-source.ts';
import type {CatchupPlan} from './sqlite-change-log-reader.ts';

const lc = createSilentLogContext();
const shard: ShardID = {appID: 'zero', shardNum: 3};
const replicaVersion = '02';
const routers: SQLiteChangeLogReadSourceRouter[] = [];
const files: DbFile[] = [];

afterEach(() => {
  for (const router of routers.splice(0)) {
    router.close();
  }
  for (const file of files.splice(0)) {
    file.delete();
  }
  vi.useRealTimers();
});

describe('SQLiteChangeLogReadSourceRouter', () => {
  test('selects a stable percentage by replica, shard, and subscriber identity', () => {
    expect(
      isSQLiteChangeLogReadSelected(shard, replicaVersion, 'task', 0),
    ).toBe(false);
    expect(
      isSQLiteChangeLogReadSelected(shard, replicaVersion, 'task', 100),
    ).toBe(true);

    const decisions = Array.from({length: 1000}, (_, i) =>
      isSQLiteChangeLogReadSelected(shard, replicaVersion, `task-${i}`, 50),
    );
    expect(decisions.filter(Boolean).length).toBeGreaterThan(400);
    expect(decisions.filter(Boolean).length).toBeLessThan(600);
    for (let i = 0; i < decisions.length; i++) {
      expect(
        isSQLiteChangeLogReadSelected(shard, replicaVersion, `task-${i}`, 50),
      ).toBe(decisions[i]);
    }

    expect(() =>
      isSQLiteChangeLogReadSelected(shard, replicaVersion, 'task', -1),
    ).toThrow('integer from 0 through 100');
    expect(() =>
      isSQLiteChangeLogReadSelected(shard, replicaVersion, 'task', 1.5),
    ).toThrow('integer from 0 through 100');
    expect(() =>
      isSQLiteChangeLogReadSelected(shard, replicaVersion, 'task', 101),
    ).toThrow('integer from 0 through 100');
  });

  test('falls back before registration for every eligibility failure', () => {
    const fixture = createFixture({now: 50, warmupStartedAtMs: 0});
    expect(fixture.router.selectForSubscriber(context())).toMatchObject({
      source: 'pg',
      reason: 'warming-up',
    });

    fixture.now = 200;
    fixture.source.state = {...fixture.source.state, schemaVersion: 13};
    expect(fixture.router.selectForSubscriber(context())).toMatchObject({
      source: 'pg',
      reason: 'schema-version',
    });

    fixture.source.state = {
      ...fixture.source.state,
      schemaVersion: 14,
      replicaVersion: 'different',
    };
    expect(fixture.router.selectForSubscriber(context())).toMatchObject({
      source: 'pg',
      reason: 'replica-version',
    });

    fixture.source.state = {...fixture.source.state, replicaVersion};
    fixture.source.planValue = {
      kind: 'too-old',
      minWatermark: '04',
      headWatermark: '08',
    };
    expect(fixture.router.selectForSubscriber(context())).toMatchObject({
      source: 'pg',
      reason: 'range-unavailable',
    });

    fixture.source.planValue = {
      kind: 'range',
      minWatermark: '02',
      headWatermark: '08',
    };
    expect(fixture.router.selectForSubscriber(context())).toMatchObject({
      source: 'sqlite',
      reason: 'eligible',
    });
  });

  test('pins snapshot bounds and consumes or expires the choice by task ID', async () => {
    const fixture = createFixture();
    const pgState = vi.fn<() => Promise<ChangeLogState>>(() =>
      Promise.resolve({replicaVersion, minWatermark: '01'}),
    );

    await expect(
      fixture.router.reserveSnapshot('sqlite-task', pgState),
    ).resolves.toEqual({replicaVersion, minWatermark: '02'});
    expect(pgState).not.toHaveBeenCalled();

    fixture.router.reportFailure(new Error('reader failed after snapshot'));
    expect(
      fixture.router.selectForSubscriber(
        context({taskID: 'sqlite-task', initial: true}),
      ),
    ).toMatchObject({
      source: 'sqlite',
      classification: 'reserved',
      pinned: true,
    });

    await expect(
      fixture.router.reserveSnapshot('expired-task', pgState),
    ).resolves.toEqual({replicaVersion, minWatermark: '01'});
    fixture.router.releaseReservation('expired-task');
    expect(
      fixture.router.selectForSubscriber(
        context({taskID: 'expired-task', initial: true}),
      ),
    ).toMatchObject({
      source: 'pg',
      classification: 'unreserved',
      reason: 'circuit-open',
      pinned: false,
    });
  });

  test('keeps a PG-selected snapshot on PG even if SQLite later becomes eligible', async () => {
    const fixture = createFixture({readPercent: 0});
    const pgState = {replicaVersion, minWatermark: '01'};
    await expect(
      fixture.router.reserveSnapshot('pg-task', () => Promise.resolve(pgState)),
    ).resolves.toEqual(pgState);

    expect(
      fixture.router.selectForSubscriber(
        context({taskID: 'pg-task', initial: true}),
      ),
    ).toMatchObject({
      source: 'pg',
      classification: 'reserved',
      pinned: true,
    });
  });

  test('reports replica eligibility while serving zero percent', () => {
    const fixture = createFixture({readPercent: 0});
    fixture.source.state = {...fixture.source.state, schemaVersion: 13};
    expect(fixture.router.selectForSubscriber(context())).toMatchObject({
      source: 'pg',
      reason: 'schema-version',
    });

    fixture.source.state = {...fixture.source.state, schemaVersion: 14};
    expect(fixture.router.selectForSubscriber(context())).toMatchObject({
      source: 'pg',
      reason: 'not-sampled',
    });
  });

  test('opens a local circuit and restores SQLite with a background probe', async () => {
    vi.useFakeTimers();
    const shared = {
      inspectError: new Error('temporarily unreadable') as Error | undefined,
      state: readState(),
    };
    const sources: TestReadSource[] = [];
    const router = new SQLiteChangeLogReadSourceRouter(
      lc,
      shard,
      replicaVersion,
      {
        replicaFile: 'unused',
        readPercent: 100,
        retentionMs: 100,
        warmupStartedAtMs: 0,
        healthProbeIntervalMs: 10,
        now: () => 1000,
        createSource: () => {
          const source = new TestReadSource(shared.state);
          source.inspectError = shared.inspectError;
          sources.push(source);
          return source;
        },
      },
    );
    routers.push(router);

    expect(router.selectForSubscriber(context())).toMatchObject({
      source: 'pg',
      reason: 'reader-error',
    });
    expect(router.selectForSubscriber(context())).toMatchObject({
      source: 'pg',
      reason: 'circuit-open',
    });

    shared.inspectError = undefined;
    await vi.advanceTimersByTimeAsync(10);
    expect(sources.at(-1)?.closed).toBe(false);
    expect(router.selectForSubscriber(context())).toMatchObject({
      source: 'sqlite',
      reason: 'eligible',
    });
  });

  test('serves a warmed process restart from a restored v14 replica', async () => {
    const file = new DbFile('sqlite-change-log-read-source');
    files.push(file);
    using db = file.connect(lc);
    db.exec(/*sql*/ `
      CREATE TABLE "_zero.versionHistory" (
        "dataVersion" INTEGER NOT NULL,
        "schemaVersion" INTEGER NOT NULL,
        "minSafeVersion" INTEGER NOT NULL,
        "lock" INTEGER PRIMARY KEY DEFAULT 1 CHECK ("lock" = 1)
      );
      INSERT INTO "_zero.versionHistory"
        ("dataVersion", "schemaVersion", "minSafeVersion")
        VALUES (14, 14, 0);
    `);
    initReplicationState(db, ['zero_data'], replicaVersion);

    const router = new SQLiteChangeLogReadSourceRouter(
      lc,
      shard,
      replicaVersion,
      {
        replicaFile: file.path,
        readPercent: 100,
        retentionMs: 100,
        warmupStartedAtMs: 0,
        now: () => 1000,
      },
    );
    routers.push(router);

    await expect(
      router.reserveSnapshot('restored-task', () =>
        Promise.resolve({replicaVersion, minWatermark: '01'}),
      ),
    ).resolves.toEqual({replicaVersion, minWatermark: replicaVersion});
    expect(
      router.selectForSubscriber(
        context({taskID: 'restored-task', initial: true}),
      ),
    ).toMatchObject({source: 'sqlite', pinned: true});
  });

  test('reports source and request classification without exposing subscriber IDs', async () => {
    const decisions: SQLiteChangeLogReadDecision[] = [];
    const fixture = createFixture({
      onDecision: decision => decisions.push(decision),
    });
    await fixture.router.reserveSnapshot('task-secret', () =>
      Promise.resolve({replicaVersion, minWatermark: '01'}),
    );
    fixture.router.selectForSubscriber(
      context({taskID: 'task-secret', initial: true}),
    );
    fixture.router.selectForSubscriber(
      context({taskID: null, id: 'subscriber-secret', initial: false}),
    );

    expect(decisions).toEqual([
      expect.objectContaining({
        source: 'sqlite',
        classification: 'snapshot',
      }),
      expect.objectContaining({
        source: 'sqlite',
        classification: 'reserved',
      }),
      expect.objectContaining({
        source: 'sqlite',
        classification: 'unreserved',
      }),
    ]);
    expect(JSON.stringify(decisions)).not.toContain('secret');
  });
});

function createFixture(
  opts: {
    readPercent?: number | undefined;
    now?: number | undefined;
    warmupStartedAtMs?: number | undefined;
    onDecision?: ((decision: SQLiteChangeLogReadDecision) => void) | undefined;
  } = {},
) {
  const source = new TestReadSource(readState());
  const fixture = {
    now: opts.now ?? 1000,
    source,
    router: undefined as unknown as SQLiteChangeLogReadSourceRouter,
  };
  fixture.router = new SQLiteChangeLogReadSourceRouter(
    lc,
    shard,
    replicaVersion,
    {
      replicaFile: 'unused',
      readPercent: opts.readPercent ?? 100,
      retentionMs: 100,
      warmupStartedAtMs: opts.warmupStartedAtMs ?? 0,
      now: () => fixture.now,
      createSource: () => source,
      onDecision: opts.onDecision,
    },
  );
  routers.push(fixture.router);
  return fixture;
}

class TestReadSource implements SQLiteChangeLogReadSource {
  state: SQLiteChangeLogReadState;
  planValue: CatchupPlan = {
    kind: 'range',
    minWatermark: '02',
    headWatermark: '08',
  };
  inspectError: Error | undefined;
  planError: Error | undefined;
  closed = false;

  constructor(state: SQLiteChangeLogReadState) {
    this.state = state;
  }

  inspect(): SQLiteChangeLogReadState {
    if (this.inspectError) {
      throw this.inspectError;
    }
    return this.state;
  }

  plan(): CatchupPlan {
    if (this.planError) {
      throw this.planError;
    }
    return this.planValue;
  }

  close(): void {
    this.closed = true;
  }
}

function readState(): SQLiteChangeLogReadState {
  return {
    schemaVersion: 14,
    replicaVersion,
    stateWatermark: '08',
    minWatermark: '02',
    headWatermark: '08',
  };
}

function context(
  overrides: Partial<SubscriberContext> = {},
): SubscriberContext {
  return {
    protocolVersion: 6,
    taskID: 'task',
    id: 'subscriber',
    mode: 'serving',
    replicaVersion,
    watermark: '02',
    initial: false,
    ...overrides,
  };
}
