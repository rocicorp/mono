import {assert, assertArray, assertString} from 'shared/src/asserts.js';

function makeChannelName(replicacheName: string): string {
  return `replicache-new-client-group:${replicacheName}`;
}

export {makeChannelName as makeChannelNameForTesting};

type NewClientChannelMessage = [clientGroupID: string];

function assertNewClientChannelMessage(
  message: unknown,
): asserts message is NewClientChannelMessage {
  assertArray(message);
  assert(message.length === 1);
  assertString(message[0]);
}

export function initNewClientChannel(
  replicacheName: string,
  signal: AbortSignal,
  clientGroupID: string,
  isNewClientGroup: boolean,
  onUpdateNeeded: () => void,
) {
  if (signal.aborted) {
    return;
  }

  const channel = new BroadcastChannel(makeChannelName(replicacheName));
  if (isNewClientGroup) {
    channel.postMessage([clientGroupID]);
  }

  channel.onmessage = (e: MessageEvent<NewClientChannelMessage>) => {
    const {data} = e;
    // Don't trust the message.
    assertNewClientChannelMessage(data);

    const [newClientGroupID] = data;
    if (newClientGroupID !== clientGroupID) {
      onUpdateNeeded();
    }
  };

  signal.addEventListener('abort', () => channel.close());
}
