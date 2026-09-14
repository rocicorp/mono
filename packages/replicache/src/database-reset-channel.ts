import {
  assertBoolean,
  assertNumber,
  assertObject,
  assertString,
} from '../../shared/src/asserts.ts';
import {BroadcastChannel} from '../../shared/src/broadcast-channel.ts';

function makeChannelName(idbName: string): string {
  return `replicache-database-reset:${idbName}`;
}

export {makeChannelName as makeDatabaseResetChannelNameForTesting};

/**
 * `dropped` tells whether the sender managed to drop the database. If it did
 * not, the database may still exist with its corrupt content, and the
 * receivers should try to drop it themselves.
 *
 * `droppedAt` is when the sender's drop attempt finished (`Date.now()`). An
 * instance created after that already opened a fresh database and must not
 * treat a successful drop as its own reset. Tabs on the same origin share the
 * wall clock, so comparing it to the receiver's creation time is sound.
 */
export type DatabaseResetMessage = {
  idbName: string;
  dropped: boolean;
  droppedAt: number;
};

function assertDatabaseResetMessage(
  value: unknown,
): asserts value is DatabaseResetMessage {
  assertObject(value);
  assertString(value.idbName);
  assertBoolean(value.dropped);
  assertNumber(value.droppedAt);
}

export type OnDatabaseReset = (dropped: boolean, droppedAt: number) => void;

/**
 * Listens for database resets from the other Replicache instances that share a
 * database (same name, schema version and format version), typically one per
 * tab.
 *
 * When one instance finds the persistent store corrupt it drops the whole
 * database. That closes the other instances' connections too, but they would
 * only see generic storage errors from their next persist or refresh. This
 * channel tells them the database was reset on purpose so they can fire
 * `onClientStateNotFound` like the instance that detected the corruption.
 *
 * Stops listening when `signal` is aborted. Use {@link notifyDatabaseReset} to
 * send.
 */
export function listenForDatabaseReset(
  idbName: string,
  signal: AbortSignal,
  onDatabaseReset: OnDatabaseReset,
): void {
  if (signal.aborted) {
    return;
  }
  const channel = new BroadcastChannel(makeChannelName(idbName));
  channel.onmessage = e => {
    const {data} = e;
    assertDatabaseResetMessage(data);
    if (data.idbName === idbName) {
      onDatabaseReset(data.dropped, data.droppedAt);
    }
  };
  signal.addEventListener('abort', () => channel.close(), {once: true});
}

/**
 * Tells the other instances sharing `idbName` that it was reset.
 *
 * This deliberately does not depend on the instance's lifetime: the write that
 * detected the corruption may still be releasing while `close()` has already
 * aborted everything else, and the other instances need to hear about the
 * drop regardless. A message posted right before the channel is closed is
 * still delivered.
 */
export function notifyDatabaseReset(
  idbName: string,
  dropped: boolean,
  droppedAt: number,
): void {
  const channel = new BroadcastChannel(makeChannelName(idbName));
  channel.postMessage({
    idbName,
    dropped,
    droppedAt,
  } satisfies DatabaseResetMessage);
  channel.close();
}
