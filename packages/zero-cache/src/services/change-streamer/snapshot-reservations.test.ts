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

  test('confirmFor() pushes a status message and confirms the reservation', async () => {
    const reservations = newReservations();
    const sub = reservations.open('task-1');
    const message = getFirstMessage(sub);

    reservations.confirmFor('task-1', 'replica-v1', 'watermark-1', 'pg');

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

  test('confirmFor() is a no-op for an already-confirmed reservation', () => {
    const reservations = newReservations();
    reservations.open('task-1');

    reservations.confirmFor('task-1', 'replica-v1', 'watermark-1', 'pg');
    reservations.confirmFor('task-1', 'replica-v1', 'watermark-2', 'pg');

    // The reservation stays pinned to the watermark from its first
    // confirmation; the second confirmFor() call is a no-op for it.
    expect(reservations.getReservedWatermarks()).toEqual(['watermark-1']);
  });

  test('the confirmation requirement clears once every task is confirmed', () => {
    const reservations = newReservations();
    reservations.open('task-1');
    reservations.open('task-2');
    expect(reservations.confirmationsRequired()).toBe(true);

    reservations.confirmFor('task-1', 'replica-v1', 'watermark-1', 'pg');
    expect(reservations.confirmationsRequired()).toBe(true);

    reservations.confirmFor('task-2', 'replica-v1', 'watermark-1', 'sqlite');
    expect(reservations.confirmationsRequired()).toBe(false);
    expect(reservations.getReservedWatermarks().sort()).toEqual([
      'watermark-1',
      'watermark-1',
    ]);
  });

  test('confirmFor() applies the pinned source bounds to one task only', async () => {
    const reservations = newReservations();
    const first = getFirstMessage(reservations.open('task-1'));
    reservations.open('task-2');

    expect(reservations.unconfirmedTaskIDs().sort()).toEqual([
      'task-1',
      'task-2',
    ]);
    reservations.confirmFor('task-1', 'replica-v1', 'sqlite-min', 'sqlite');

    expect(await first).toMatchObject([
      'status',
      {replicaVersion: 'replica-v1', minWatermark: 'sqlite-min'},
    ]);
    expect(reservations.unconfirmedTaskIDs()).toEqual(['task-2']);
    expect(reservations.getReservedWatermarks()).toEqual(['sqlite-min']);
  });

  test('a reservation opened after a confirmation is still unconfirmed', () => {
    const reservations = newReservations();
    reservations.open('task-1');
    reservations.confirmFor('task-1', 'replica-v1', 'watermark-1', 'pg');

    reservations.open('task-2');
    expect(reservations.confirmationsRequired()).toBe(true);
    expect(reservations.getReservedWatermarks()).toEqual(['watermark-1']);

    reservations.confirmFor('task-2', 'replica-v1', 'watermark-2', 'pg');
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
    expect(reservations.isCurrent('task-1', sub1)).toBe(false);
    expect(reservations.isCurrent('task-1', sub2)).toBe(true);

    // Only one reservation is tracked for the taskID, and it's the new one:
    // it still receives the confirmation push.
    const message = getFirstMessage(sub2);
    reservations.confirmFor('task-1', 'replica-v1', 'watermark-1', 'pg');
    await message;
    expect(reservations.getReservedWatermarks()).toEqual(['watermark-1']);
  });

  test('cancelling a superseded reservation leaves the replacement intact', async () => {
    // The onClose callback stands in for the purge-pause release the
    // change-streamer wires up: it must fire once per closed reservation,
    // never for a superseded instance's late cancel.
    const closed: string[] = [];
    const reservations = new SnapshotReservations(
      createSilentLogContext(),
      {backupURL: 's3://foo/bar', litestreamVersion: 'v5'},
      taskID => closed.push(taskID),
    );
    const sub1 = reservations.open('task-1');
    const sub2 = reservations.open('task-1');
    expect(closed).toEqual(['task-1']);

    // The superseded caller's failure path cancels the downstream it owns.
    // The instance guard makes it a no-op: the replacement stays open, and
    // its purge pause is not released a second time.
    sub1.cancel();
    expect(closed).toEqual(['task-1']);
    expect(reservations.confirmationsRequired()).toBe(true);

    const message = getFirstMessage(sub2);
    reservations.confirmFor('task-1', 'replica-v1', 'watermark-1', 'pg');
    await message;
    expect(reservations.getReservedWatermarks()).toEqual(['watermark-1']);

    reservations.close('task-1');
    expect(closed).toEqual(['task-1', 'task-1']);
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

  test('confirmFor() an unknown taskID is a no-op', () => {
    const reservations = newReservations();
    expect(() =>
      reservations.confirmFor('task-1', 'replica-v1', 'watermark-1', 'pg'),
    ).not.toThrow();
  });

  test('noteConfirmationDelayed() reports once per reservation', () => {
    const reservations = newReservations();
    reservations.open('task-1');

    // Confirmation is retried on every backup. Only the first deferral for a
    // reservation counts, so the metric measures followers, not backups.
    expect(reservations.noteConfirmationDelayed('task-1')).toBe(true);
    expect(reservations.noteConfirmationDelayed('task-1')).toBe(false);
    expect(reservations.noteConfirmationDelayed('task-1')).toBe(false);

    // A replacement reservation for the same task is a new follower.
    reservations.open('task-1');
    expect(reservations.noteConfirmationDelayed('task-1')).toBe(true);
  });

  test('noteConfirmationDelayed() is false for an unknown taskID', () => {
    const reservations = newReservations();
    expect(reservations.noteConfirmationDelayed('unknown-task')).toBe(false);
  });
});
