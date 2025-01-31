import {resolver} from '@rocicorp/resolver';
import EventEmitter from 'node:events';
import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import {
  ProcessManager,
  runUntilKilled,
  type WorkerType,
} from '../services/life-cycle.ts';
import type {SingletonService} from '../services/service.ts';
import {inProcChannel} from '../types/processes.ts';

describe('shutdown', () => {
  const lc = createSilentLogContext();
  let proc: EventEmitter;
  let processes: ProcessManager;
  let events: string[];
  let changeStreamer: TestWorker;
  let replicator: TestWorker;
  let syncer1: TestWorker;
  let syncer2: TestWorker;
  let all: TestWorker[];

  class TestWorker implements SingletonService {
    readonly id: string;
    readonly type: WorkerType;
    draining = resolver();
    finishDrain = resolver();
    running = resolver();
    stopped = resolver();

    constructor(id: string, type: WorkerType) {
      this.id = id;
      this.type = type;
    }

    run() {
      this.running.resolve();
      return this.stopped.promise;
    }

    drain(): Promise<void> {
      events.push(`drain ${this.type}`);
      this.draining.resolve();
      return this.finishDrain.promise;
    }

    stop() {
      events.push(`stop ${this.type}`);
      this.stopped.resolve();
      return promiseVoid;
    }
  }

  function startWorker(id: string, type: WorkerType): TestWorker {
    const worker = new TestWorker(id, type);
    const [parentPort, childPort] = inProcChannel();

    processes.addWorker(parentPort, type, id);

    void runUntilKilled(lc, childPort, worker).then(
      () => parentPort.emit('close', 0),
      () => parentPort.emit('close', -1),
    );
    return worker;
  }

  beforeEach(async () => {
    // For testing process.exit()
    process.env['SINGLE_PROCESS'] = '1';

    proc = new EventEmitter();
    processes = new ProcessManager(lc, proc);
    events = [];
    changeStreamer = startWorker('cs', 'supporting');
    replicator = startWorker('rep', 'supporting');
    syncer1 = startWorker('s1', 'user-facing');
    syncer2 = startWorker('s2', 'user-facing');

    all = [changeStreamer, replicator, syncer1, syncer2];

    await Promise.all(all.map(w => w.running.promise));
  });

  test.each([['SIGTERM'], ['SIGINT']])(
    'graceful shutdown: %s',
    async signal => {
      proc.emit(signal);

      await syncer1.draining.promise;
      await syncer2.draining.promise;

      syncer1.finishDrain.resolve();
      syncer2.finishDrain.resolve();

      await changeStreamer.draining.promise;
      await replicator.draining.promise;

      changeStreamer.finishDrain.resolve();
      replicator.finishDrain.resolve();

      await Promise.all(all.map(w => w.stopped.promise));

      expect(events).toEqual([
        'drain user-facing',
        'drain user-facing',
        'stop user-facing',
        'stop user-facing',
        'drain supporting',
        'drain supporting',
        'stop supporting',
        'stop supporting',
      ]);
    },
  );

  test.each([['SIGTERM'], ['SIGINT']])(
    'error during graceful shutdown: %s',
    async signal => {
      proc.emit(signal);

      await syncer1.draining.promise;
      await syncer2.draining.promise;

      syncer1.stopped.reject('doh');
      syncer2.finishDrain.resolve();

      await changeStreamer.draining.promise;
      await replicator.draining.promise;

      changeStreamer.finishDrain.resolve();
      replicator.finishDrain.resolve();

      await Promise.allSettled(all.map(w => w.stopped.promise));

      expect(events).toEqual([
        'drain user-facing',
        'drain user-facing',
        'stop user-facing',
        'drain supporting',
        'drain supporting',
        'stop supporting',
        'stop supporting',
      ]);
    },
  );

  test.each([['SIGTERM'], ['SIGINT']])(
    'all error during graceful shutdown: %s',
    async signal => {
      proc.emit(signal);

      await syncer1.draining.promise;
      await syncer2.draining.promise;

      syncer1.stopped.reject('doh');
      syncer2.stopped.reject('doh');

      await changeStreamer.draining.promise;
      await replicator.draining.promise;

      changeStreamer.finishDrain.resolve();
      replicator.finishDrain.resolve();

      await Promise.allSettled(all.map(w => w.stopped.promise));

      expect(events).toEqual([
        'drain user-facing',
        'drain user-facing',
        'drain supporting',
        'drain supporting',
        'stop supporting',
        'stop supporting',
      ]);
    },
  );

  test.each([
    [
      'SIGQUIT',
      () => proc.emit('SIGQUIT'),
      [
        'stop supporting',
        'stop supporting',
        'stop user-facing',
        'stop user-facing',
      ],
    ],
    [
      'supporting worker exits',
      () => replicator.stop(),
      [
        'stop supporting',
        'stop supporting',
        'stop user-facing',
        'stop user-facing',
      ],
    ],
    [
      'supporting worker error',
      () => changeStreamer.stopped.reject('foo'),
      ['stop supporting', 'stop user-facing', 'stop user-facing'],
    ],
    [
      'user-facing worker exits',
      () => syncer1.stop(),
      [
        'stop supporting',
        'stop supporting',
        'stop user-facing',
        'stop user-facing',
      ],
    ],
    [
      'user-facing worker error',
      () => syncer2.stopped.reject('foo'),
      ['stop supporting', 'stop supporting', 'stop user-facing'],
    ],
  ])('forceful shutdown: %s', async (_name, fn, expectedEvents) => {
    void fn();

    await Promise.allSettled(all.map(w => w.stopped.promise));

    // sort() because order doesn't matter.
    expect(events.sort()).toEqual(expectedEvents.sort());
  });

  test('graceful shutdown with no user-facing workers', async () => {
    proc = new EventEmitter();
    processes = new ProcessManager(lc, proc);
    const changeStreamer = startWorker('cs', 'supporting');
    const replicator = startWorker('rep', 'supporting');
    const all = [changeStreamer, replicator];

    await Promise.all(all.map(w => w.running.promise));

    void changeStreamer.stop();

    await replicator.draining.promise;

    replicator.finishDrain.resolve();

    await replicator.stopped.promise;

    lc.debug?.('expecting');
    expect(events).toEqual([
      'stop supporting',
      'drain supporting',
      'stop supporting',
    ]);
  });
});
