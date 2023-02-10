import {
  Client,
  ClientMap,
  ClientDD31,
  ClientSDD,
  setClients,
  getClients,
  initClientDD31,
  initClientSDD,
} from './clients.js';
import type * as dag from '../dag/mod.js';
import type * as sync from '../sync/mod.js';
import {LogContext} from '@rocicorp/logger';
import type {IndexDefinitions} from '../index-defs.js';

export function setClientsForTesting(
  clients: ClientMap,
  dagStore: dag.Store,
): Promise<ClientMap> {
  return dagStore.withWrite(async dagWrite => {
    await setClients(clients, dagWrite);
    await dagWrite.commit();
    return clients;
  });
}

type PartialClient = Partial<Client> &
  Pick<Client, 'heartbeatTimestampMs' | 'headHash'>;

type PartialClientSDD = Partial<ClientSDD> &
  Pick<ClientSDD, 'heartbeatTimestampMs' | 'headHash'>;

type PartialClientDD31 = Partial<ClientDD31> &
  Pick<ClientDD31, 'heartbeatTimestampMs' | 'headHash'>;

export function makeClient(partialClient: PartialClient): Client {
  const p31 = partialClient as PartialClientDD31;
  if (typeof p31.clientGroupID === 'string') {
    // Forced DD31 path
    return {
      clientGroupID: p31.clientGroupID,
      headHash: p31.headHash,
      heartbeatTimestampMs: p31.heartbeatTimestampMs,
      tempRefreshHash: p31.tempRefreshHash ?? null,
    };
  }

  const pSDD = partialClient as PartialClientSDD;
  if (
    typeof pSDD.mutationID === 'number' ||
    typeof pSDD.lastServerAckdMutationID === 'number'
  ) {
    // Forced SDD path
    return {
      mutationID: 0,
      lastServerAckdMutationID: 0,
      ...partialClient,
    };
  }

  if (DD31) {
    return {
      clientGroupID: 'make-client-group-id',
      headHash: partialClient.headHash,
      heartbeatTimestampMs: partialClient.heartbeatTimestampMs,
      tempRefreshHash: null,
    };
  }

  // SDD
  return {
    mutationID: 0,
    lastServerAckdMutationID: 0,
    ...partialClient,
  };
}

export function makeClientMap(
  obj: Record<sync.ClientID, PartialClient>,
): ClientMap {
  return new Map(
    Object.entries(obj).map(
      ([id, client]) => [id, makeClient(client)] as const,
    ),
  );
}

export async function deleteClientForTesting(
  clientID: sync.ClientID,
  dagStore: dag.Store,
): Promise<void> {
  await dagStore.withWrite(async dagWrite => {
    const clients = new Map(await getClients(dagWrite));
    clients.delete(clientID);
    await setClients(clients, dagWrite);
    await dagWrite.commit();
  });
}

export async function initClientWithClientID(
  clientID: sync.ClientID,
  dagStore: dag.Store,
  mutatorNames: string[],
  indexes: IndexDefinitions,
  dd31: boolean,
): Promise<void> {
  let generatedClientID, client, clientMap;
  if (dd31) {
    [generatedClientID, client, clientMap] = await initClientDD31(
      new LogContext(),
      dagStore,
      mutatorNames,
      indexes,
    );
  } else {
    [generatedClientID, client, clientMap] = await initClientSDD(dagStore);
  }
  const newMap = new Map(clientMap);
  newMap.delete(generatedClientID);
  newMap.set(clientID, client);
  await setClientsForTesting(newMap, dagStore);
}
