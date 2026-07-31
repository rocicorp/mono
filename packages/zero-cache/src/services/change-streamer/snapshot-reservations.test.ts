import {resolver} from '@rocicorp/resolver';
import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Source} from '../../types/streams.ts';
import {SnapshotReservations} from './snapshot-reservations.ts';
import type {SnapshotMessage} from './snapshot.ts';

function getFirstMessage(
  sub: Source<SnapshotMessage>,
): Promise<SnapshotMessage> {
  const {promise, resolve} = resolver<SnapshotMessage>();
  void (async function () {
    for await (const msg of sub) {
      resolve(msg);
      // Simulate an open connection; do not exit the loop.
    }
  })();
  return promise;
}

// `open()` only exposes the `Source` interface (not the full `Subscription`),
// so cancellation is observed the same way a real consumer would: a
// cancelled subscription's iteration completes (`done: true`) immediately,
// without hanging, since cancel() terminates any in-flight `next()` call.
function isCancelled(sub: Source<SnapshotMessage>): Promise<boolean> {
  return sub[Symbol.asyncIterator]()
    .next()
    .then(({done}) => !!done);
}

describe('change-streamer/snapshot-reservations', () => {
  function newReservations() {
    return new SnapshotReservations(createSilentLogContext(), {
      backupURL: 's3://foo/bar',
      litestreamVersion: 'v5',
    });
  }

  test('confirmationsRequired() is false with no reservations', () => {
    const reservations = newReservations();
    expect(reservations.confirmationsRequired()).toBe(false);
  });

  test('open() creates an unconfirmed reservation', () => {
    const reservations = newReservations();
    reservations.open('task-1');

    expect(reservations.confirmationsRequired()).toBe(true);
    expect(reservations.getReservedWatermarks()).toEqual([]);
  });

  test('confirm() pushes a status message and confirms the reservation', async () => {
    const reservations = newReservations();
    const sub = reservations.open('task-1');
    const message = getFirstMessage(sub);

    reservations.confirm('replica-v1', 'watermark-1');

    expect(await message).toEqual([
      'status',
      {
        tag: 'status',
        backupURL: 's3://foo/bar',
        replicaVersion: 'replica-v1',
        minWatermark: 'watermark-1',
      },
    ]);
    expect(reservations.confirmationsRequired()).toBe(false);
    expect(reservations.getReservedWatermarks()).toEqual(['watermark-1']);
  });

  test('confirm() is a no-op for an already-confirmed reservation', () => {
    const reservations = newReservations();
    reservations.open('task-1');

    reservations.confirm('replica-v1', 'watermark-1');
    reservations.confirm('replica-v1', 'watermark-2');

    // The reservation stays pinned to the watermark from its first
    // confirmation; the second confirm() call is a no-op for it.
    expect(reservations.getReservedWatermarks()).toEqual(['watermark-1']);
  });

  test('a single confirm() call resolves all pending reservations at once', () => {
    const reservations = newReservations();
    reservations.open('task-1');
    reservations.open('task-2');
    expect(reservations.confirmationsRequired()).toBe(true);

    reservations.confirm('replica-v1', 'watermark-1');

    expect(reservations.confirmationsRequired()).toBe(false);
    expect(reservations.getReservedWatermarks().sort()).toEqual([
      'watermark-1',
      'watermark-1',
    ]);
  });

  test('confirm() only resolves reservations opened before it was called', () => {
    const reservations = newReservations();
    reservations.open('task-1');
    reservations.confirm('replica-v1', 'watermark-1');

    // Opened after the first confirm(); still unconfirmed.
    reservations.open('task-2');
    expect(reservations.confirmationsRequired()).toBe(true);
    expect(reservations.getReservedWatermarks()).toEqual(['watermark-1']);

    reservations.confirm('replica-v1', 'watermark-2');
    expect(reservations.confirmationsRequired()).toBe(false);
    expect(reservations.getReservedWatermarks().sort()).toEqual([
      'watermark-1',
      'watermark-2',
    ]);
  });

  test('open() with the same taskID cancels the previous reservation', async () => {
    const reservations = newReservations();
    const sub1 = reservations.open('task-1');
    const sub2 = reservations.open('task-1');

    expect(await isCancelled(sub1)).toBe(true);

    // Only one reservation is tracked for the taskID, and it's the new one:
    // it still receives the confirm() push.
    const message = getFirstMessage(sub2);
    reservations.confirm('replica-v1', 'watermark-1');
    await message;
    expect(reservations.getReservedWatermarks()).toEqual(['watermark-1']);
  });

  test('close() cancels and removes the reservation', async () => {
    const reservations = newReservations();
    const sub = reservations.open('task-1');

    reservations.close('task-1');

    expect(await isCancelled(sub)).toBe(true);
    expect(reservations.confirmationsRequired()).toBe(false);
  });

  test('close() is a no-op for an unknown taskID', () => {
    const reservations = newReservations();
    reservations.open('task-1');

    expect(() => reservations.close('unknown-task')).not.toThrow();
    expect(reservations.confirmationsRequired()).toBe(true);
  });

  test('cancelling a subscription closes its reservation via cleanup', () => {
    const reservations = newReservations();
    const sub = reservations.open('task-1');

    sub.cancel();

    expect(reservations.confirmationsRequired()).toBe(false);
    expect(reservations.getReservedWatermarks()).toEqual([]);
  });

  test('confirm() with no reservations is a no-op', () => {
    const reservations = newReservations();
    expect(() =>
      reservations.confirm('replica-v1', 'watermark-1'),
    ).not.toThrow();
  });
});
