import type {PullRequestBody, PullResponseMessage} from 'reflect-protocol';
import {assert} from 'shared/src/asserts.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import {
  IncludeDeleted,
  listClientRecordsForClientGroup,
} from '../types/client-record.js';
import type {ClientID, Socket} from '../types/client-state.js';
import {compareVersions, getVersion} from '../types/version.js';
import {send} from '../util/socket.js';

export async function handlePull(
  storage: DurableStorage,
  pullRequest: PullRequestBody,
  ws: Socket,
): Promise<void> {
  const {clientGroupID, cookie, requestID} = pullRequest;
  // Include deleted client records so that we can update the pending mutations
  // on the client.
  const records = await listClientRecordsForClientGroup(
    clientGroupID,
    IncludeDeleted.Include,
    storage,
  );
  const lastMutationIDChanges: Record<ClientID, number> = {};
  for (const [clientID, record] of records) {
    assert(record.clientGroupID === clientGroupID);
    if (
      record.lastMutationIDVersion !== null &&
      compareVersions(cookie, record.lastMutationIDVersion) < 0
    ) {
      lastMutationIDChanges[clientID] = record.lastMutationID;
    }
  }
  const version = await getVersion(storage);
  const pullResponseMessage: PullResponseMessage = [
    'pull',
    {
      cookie: version || 0,
      lastMutationIDChanges,
      requestID,
    },
  ];
  send(ws, pullResponseMessage);
}
