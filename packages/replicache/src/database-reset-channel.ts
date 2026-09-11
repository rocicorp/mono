import {assertObject, assertString} from '../../shared/src/asserts.ts';
import {BroadcastChannel} from '../../shared/src/broadcast-channel.ts';

function makeChannelName(idbName: string): string {
  return `replicache-database-reset:${idbName}`;
}

export {makeChannelName as makeDatabaseResetChannelNameForTesting};

type DatabaseResetMessage = {idbName: string};

function assertDatabaseResetMessage(
  value: unknown,
): asserts value is DatabaseResetMessage {
  assertObject(value);
  assertString(value.idbName);
}

/**
 * Coordinates a database reset between the Replicache instances that share a
 * database (same name, schema version and format version), typically one per
 * tab.
 *
 * When one instance finds the persistent store corrupt it drops the whole
 * database. That closes the other instances' connections too, but they would
 * only see generic storage errors from their next persist or refresh. This
 * channel tells them the database was reset on purpose so they can fire
 * `onClientStateNotFound` like the instance that detected the corruption.
 *
 * Returns a function that broadcasts the reset to the other instances.
 */
export function initDatabaseResetChannel(
  idbName: string,
  signal: AbortSignal,
  onDatabaseReset: () => void,
): () => void {
  if (signal.aborted) {
    return () => undefined;
  }
  const channel = new BroadcastChannel(makeChannelName(idbName));

  channel.onmessage = e => {
    const {data} = e;
    assertDatabaseResetMessage(data);
    if (data.idbName === idbName) {
      onDatabaseReset();
    }
  };

  signal.addEventListener('abort', () => channel.close(), {once: true});

  return () => {
    if (signal.aborted) {
      return;
    }
    channel.postMessage({idbName} satisfies DatabaseResetMessage);
  };
}
