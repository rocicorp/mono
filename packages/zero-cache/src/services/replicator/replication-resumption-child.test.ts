import {EventEmitter} from 'node:events';
import {describe, expect, test, vi} from 'vitest';
import {promiseOrAbort} from '../../../../shared/src/promise-race.ts';
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

describe('replication-resumption-child/signal', () => {
  const never = () => new Promise<string>(() => {});

  test('other resolves first', async () => {
    const source = new IPCDownstreamSource(fakeWorker());
    expect(await promiseOrAbort(Promise.resolve('foo'), source.signal)).toBe(
      'foo',
    );
  });

  test('local cancel() resolves signal', async () => {
    const source = new IPCDownstreamSource(fakeWorker());
    const raced = promiseOrAbort(never(), source.signal);
    source.cancel();
    await expect(raced).rejects.toThrowErrorMatchingInlineSnapshot(
      `[AbortError: This operation was aborted]`,
    );
  });

  test('producer source-end resolves signal', async () => {
    const worker = fakeWorker();
    const source = new IPCDownstreamSource(worker);
    const raced = promiseOrAbort(never(), source.signal);
    worker.emit('message', ['replication-resumption:source-end', {}]);
    await expect(raced).rejects.toThrowErrorMatchingInlineSnapshot(
      `[AbortError: This operation was aborted]`,
    );
  });

  test('producer source-error rejects sigal with the error', async () => {
    const worker = fakeWorker();
    const source = new IPCDownstreamSource(worker);
    const raced = promiseOrAbort(never(), source.signal);
    worker.emit('message', [
      'replication-resumption:source-error',
      {message: 'upstream-boom'},
    ]);
    await expect(raced).rejects.toThrow('upstream-boom');
  });

  test('signal after source-error rejects immediately', async () => {
    const worker = fakeWorker();
    const source = new IPCDownstreamSource(worker);
    worker.emit('message', [
      'replication-resumption:source-error',
      {message: 'upstream-boom'},
    ]);
    await expect(promiseOrAbort(never(), source.signal)).rejects.toThrow(
      'upstream-boom',
    );
  });

  test('signal after source-end resolves immediately', async () => {
    const worker = fakeWorker();
    const source = new IPCDownstreamSource(worker);
    worker.emit('message', ['replication-resumption:source-end', {}]);
    await expect(
      promiseOrAbort(never(), source.signal),
    ).rejects.toThrowErrorMatchingInlineSnapshot(
      `[AbortError: This operation was aborted]`,
    );
  });
});
