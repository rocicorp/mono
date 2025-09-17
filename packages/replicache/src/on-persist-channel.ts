/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members, @typescript-eslint/prefer-promise-reject-errors */
import {assertObject, assertString} from '../../shared/src/asserts.ts';
import {BroadcastChannel} from '../../shared/src/broadcast-channel.ts';
import type {ClientGroupID, ClientID} from './sync/ids.ts';

function makeChannelName(replicacheName: string): string {
  return `replicache-on-persist:${replicacheName}`;
}

export type PersistInfo = {
  clientGroupID: ClientGroupID;
  clientID: ClientID;
};

export type OnPersist = (persistInfo: PersistInfo) => void;

type HandlePersist = OnPersist;

function assertPersistInfo(value: unknown): asserts value is PersistInfo {
  assertObject(value);
  assertString(value.clientGroupID);
  assertString(value.clientID);
}

export function initOnPersistChannel(
  replicacheName: string,
  signal: AbortSignal,
  handlePersist: HandlePersist,
): OnPersist {
  if (signal.aborted) {
    return () => undefined;
  }
  const channel = new BroadcastChannel(makeChannelName(replicacheName));

  channel.onmessage = e => {
    const {data} = e;
    assertPersistInfo(data);
    handlePersist({
      clientGroupID: data.clientGroupID,
      clientID: data.clientID,
    });
  };

  signal.addEventListener('abort', () => channel.close(), {once: true});

  return (persistInfo: PersistInfo) => {
    if (signal.aborted) {
      return;
    }
    channel.postMessage(persistInfo);
    handlePersist(persistInfo);
  };
}
