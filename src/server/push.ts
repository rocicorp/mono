import type {ClientID, ClientState} from '../types/client-state.js';
import type {PushBody} from '../protocol/push.js';
import type {LogContext} from '@rocicorp/logger';
import type {TurnBuffer} from './turn-buffer.js';

export type Now = () => number;
export type ProcessUntilDone = () => void;

/**
 * handles the 'push' upstream message by queueing the mutations included in
 * [[body]] in the appropriate client state.
 */
export function handlePush(
  lc: LogContext,
  clientID: ClientID,
  client: ClientState,
  body: PushBody,
  turnBuffer: TurnBuffer,
  now: Now,
  processUntilDone: ProcessUntilDone,
) {
  lc = lc.addContext('requestID', body.requestID);
  const nowTimestamp = now();
  lc.debug?.('handling push', JSON.stringify(body));
  if (client.clockBehindByMs === undefined) {
    client.clockBehindByMs = nowTimestamp - body.timestamp;
    lc.debug?.(
      'initializing clock offset: clock behind by',
      client.clockBehindByMs,
    );
  }

  lc.info?.(
    'push timing',
    nowTimestamp,
    '-',
    body.unixTimestamp,
    '=',
    nowTimestamp - body.unixTimestamp,
  );

  turnBuffer.addMutations(body.mutations, clientID, body.unixTimestamp);

  // for (const m of body.mutations) {
  //   let idx = client.pending.findIndex(pm => pm.id >= m.id);
  //   if (idx === -1) {
  //     idx = client.pending.length;
  //   } else if (client.pending[idx].id === m.id) {
  //     lc.debug?.('mutation already been queued', m.id);
  //     continue;
  //   }
  //   // Just use client timestamps
  //   //m.timestamp += client.clockBehindByMs;
  //   lc.info?.(
  //     'push mutation timing',
  //     nowTimestamp,
  //     '-',
  //     m.unixTimestamp,
  //     '=',
  //     nowTimestamp - m.unixTimestamp,
  //   );
  //   client.pending.splice(idx, 0, {
  //     ...m,
  //     receivedTimestamp: nowTimestamp,
  //   });
  //   lc.debug?.(
  //     'inserted mutation, pending is now',
  //     JSON.stringify(client.pending),
  //   );
  // }

  processUntilDone();
}
