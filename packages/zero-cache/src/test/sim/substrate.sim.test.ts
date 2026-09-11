import {stat} from 'node:fs';
import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {LogContext} from '@rocicorp/logger';
import {afterEach, describe, expect, test} from 'vitest';
import {Database} from '../../../../zqlite/src/db.ts';
import {initReplicationState} from '../../services/replicator/schema/replication-state.ts';
import {ReplicationMessages} from '../../services/replicator/test-utils.ts';
import {SimClock} from './clock.ts';
import {DeterminismGuard} from './guard.ts';
import {inThreadWriteWorker} from './in-thread-worker.ts';
import {currentIncarnation, Incarnation} from './incarnation.ts';
import {Trace} from './trace.ts';

const EPOCH_MS = Date.UTC(2026, 0, 1);

describe('sim/substrate', () => {
  let clock: SimClock | undefined;
  let guard: DeterminismGuard | undefined;
  const dirs: string[] = [];

  afterEach(() => {
    guard?.disable();
    guard = undefined;
    clock?.uninstall();
    clock = undefined;
    for (const dir of dirs.splice(0)) {
      rmSync(dir, {recursive: true, force: true});
    }
  });

  function install() {
    const scope = new Incarnation('run', 0);
    guard = new DeterminismGuard(
      () => currentIncarnation() !== undefined,
    ).enable();
    clock = new SimClock(EPOCH_MS, {yielding: () => guard?.expectImmediate()});
    clock.install();
    return {clock, guard, scope};
  }

  test('timers keep their incarnation, and die with it', async () => {
    const {clock} = install();
    const a = new Incarnation('a', 1);
    const b = new Incarnation('b', 1);
    const fired: string[] = [];
    a.run(() => {
      setTimeout(() => fired.push(`once:${currentIncarnation()?.name}`), 10);
      setInterval(() => fired.push(`every:${currentIncarnation()?.name}`), 4);
    });
    b.run(() =>
      setTimeout(() => fired.push(`once:${currentIncarnation()?.name}`), 10),
    );

    await clock.advance(9);
    expect(fired).toEqual(['every:a#1', 'every:a#1']);
    expect(clock.pendingTimers(a)).toBe(2);

    a.fence();
    expect(clock.pendingTimers(a)).toBe(0);
    await clock.advance(20);
    expect(fired).toEqual(['every:a#1', 'every:a#1', 'once:b#1']);

    a.run(() => setTimeout(() => fired.push('dead'), 1));
    await clock.advance(5);
    expect(fired).not.toContain('dead');
  });

  test('promise continuations keep their incarnation', async () => {
    const {clock} = install();
    const a = new Incarnation('a', 1);
    const seen: (string | undefined)[] = [];
    let release = () => {};
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    const done = a.run(async () => {
      await gate;
      seen.push(currentIncarnation()?.name);
      await new Promise(resolve => setTimeout(resolve, 5));
      seen.push(currentIncarnation()?.name);
    });
    // Released from outside any incarnation.
    release();
    await clock.settle();
    expect(seen).toEqual(['a#1']);
    await clock.advance(5);
    await done;
    expect(seen).toEqual(['a#1', 'a#1']);
  });

  test('Date and performance are virtual', async () => {
    const {clock} = install();
    const date = Date.now();
    const perf = performance.now();
    expect(date).toBe(EPOCH_MS);
    await clock.advance(1234);
    expect(Date.now() - date).toBe(1234);
    expect(performance.now() - perf).toBe(1234);
    expect(clock.elapsed()).toBe(1234);
  });

  test('the guard allows virtual timers and reports real I/O', async () => {
    const {clock, guard, scope} = install();
    await scope.run(async () => {
      let fired = 0;
      setTimeout(() => fired++, 5);
      setImmediate(() => fired++);
      await clock.advance(10);
      await clock.settle();
      expect(fired).toBe(2);
    });
    expect(guard.takeViolations()).toEqual([]);

    await scope.run(
      () =>
        new Promise<void>(resolve =>
          stat(import.meta.filename, () => resolve()),
        ),
    );
    expect(guard.takeViolations().map(({type}) => type)).toEqual([
      'FSREQCALLBACK',
    ]);
  });

  test('equal runs hash equally', () => {
    const run = (value: number) => {
      const trace = new Trace({now: () => 0, runDirs: ['/tmp/run-a']});
      trace.emit('rm', 1, 'action', {path: '/tmp/run-a/replica.db', value});
      new LogContext('debug', {component: 'x'}, trace.sink('rm', 1)).info?.(
        'hello',
        new Error('at /tmp/run-a/replica.db'),
        5n,
      );
      return trace.hash;
    };
    expect(run(1)).toBe(run(1));
    expect(run(1)).not.toBe(run(2));
  });

  test('an in-thread write worker applies a transaction', async () => {
    const {clock, guard, scope} = install();
    const dir = mkdtempSync(join(tmpdir(), 'sim-substrate-'));
    dirs.push(dir);
    const trace = new Trace({now: () => clock.elapsed(), runDirs: [dir]});
    const lc = new LogContext('debug', {}, trace.sink('vs', 1));
    const file = join(dir, 'replica.db');
    const setup = new Database(lc, file);
    setup.pragma('journal_mode = wal');
    initReplicationState(setup, ['zero_data'], '02');
    setup.exec(
      'CREATE TABLE issues(id INTEGER, _0_version TEXT); ' +
        'CREATE UNIQUE INDEX issues_pk ON issues(id);',
    );
    setup.close();

    const issues = new ReplicationMessages({issues: 'id'});
    const committed = await scope.run(async () => {
      const worker = inThreadWriteWorker({
        createLogContext: () => lc,
        createLitestreamClient: () => {
          throw new Error('no checkpointer');
        },
      });
      await worker.init(
        file,
        'serving',
        {busyTimeout: 0, analysisLimit: 1000},
        {level: 'debug', format: 'text'},
        null,
      );
      const result = await worker.processMessages([
        ['begin', issues.begin(), {commitWatermark: '03'}],
        ['data', issues.insert('issues', {id: 1})],
        ['commit', issues.commit(), {watermark: '03'}],
      ]);
      const state = await worker.getSubscriptionState();
      await worker.stop();
      return {result, state};
    });
    expect(committed.result?.watermark).toBe('03');
    expect(committed.state.watermark).toBe('03');
    expect(guard.takeViolations()).toEqual([]);
  });
});
