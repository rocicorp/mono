import {EventEmitter} from 'node:events';
import {describe, expect, test, vi} from 'vitest';
import type * as processes from '../../types/processes.ts';
import type {Worker} from '../../types/processes.ts';

// The module self-starts its worker loop on import when `parentWorker` is set
// (i.e. `process.send` exists, which is true under vitest's forked pool). Mock
// it to null so importing the module for unit tests has no side effects.
vi.mock('../../types/processes.ts', async importOriginal => ({
  ...(await importOriginal<typeof processes>()),
  parentWorker: null,
}));

const {IPCDownstreamSource} = await import('./replication-resumption-child.ts');

/**
 * A minimal stand-in for the parent {@link Worker} that only supports the
 * `on`/`off`/`send` surface used by {@link IPCDownstreamSource}.
 */
function fakeWorker(): Worker & {sent: unknown[]} {
  const ee = new EventEmitter() as EventEmitter & {sent: unknown[]};
  ee.sent = [];
  (ee as unknown as {send: (msg: unknown) => boolean}).send = msg => {
    ee.sent.push(msg);
    return true;
  };
  return ee as unknown as Worker & {sent: unknown[]};
}

describe('replication-resumption-child/doneOr', () => {
  const never = () => new Promise<string>(() => {});

  test('other resolves first', async () => {
    const source = new IPCDownstreamSource(fakeWorker());
    expect(await source.doneOr(Promise.resolve('foo'))).toBe('foo');
  });

  test('local cancel() resolves doneOr', async () => {
    const source = new IPCDownstreamSource(fakeWorker());
    const raced = source.doneOr(never());
    source.cancel();
    expect(await raced).toBeUndefined();
  });

  test('producer source-end resolves doneOr', async () => {
    const worker = fakeWorker();
    const source = new IPCDownstreamSource(worker);
    const raced = source.doneOr(never());
    worker.emit('message', ['replication-resumption:source-end', {}]);
    expect(await raced).toBeUndefined();
  });

  test('producer source-error rejects doneOr with the error', async () => {
    const worker = fakeWorker();
    const source = new IPCDownstreamSource(worker);
    const raced = source.doneOr(never());
    worker.emit('message', [
      'replication-resumption:source-error',
      {message: 'upstream-boom'},
    ]);
    await expect(raced).rejects.toThrow('upstream-boom');
  });

  test('doneOr after source-error rejects immediately', async () => {
    const worker = fakeWorker();
    const source = new IPCDownstreamSource(worker);
    worker.emit('message', [
      'replication-resumption:source-error',
      {message: 'upstream-boom'},
    ]);
    await expect(source.doneOr(never())).rejects.toThrow('upstream-boom');
  });

  test('doneOr after source-end resolves immediately', async () => {
    const worker = fakeWorker();
    const source = new IPCDownstreamSource(worker);
    worker.emit('message', ['replication-resumption:source-end', {}]);
    expect(await source.doneOr(never())).toBeUndefined();
  });

  test('unrecognized messages do not terminate doneOr', async () => {
    const worker = fakeWorker();
    const source = new IPCDownstreamSource(worker);
    const resolveWith = vi.fn();
    const raced = source.doneOr(never()).then(resolveWith);
    worker.emit('message', ['some-other-message', {}]);
    worker.emit('message', 'not-even-an-array');
    await Promise.resolve();
    expect(resolveWith).not.toHaveBeenCalled();
    source.cancel(); // settle the outstanding race so the test ends cleanly.
    await raced;
  });
});
