/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members, @typescript-eslint/prefer-promise-reject-errors */
import {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import type {Enum} from '../../../shared/src/enum.ts';
import type {Store} from '../dag/store.ts';
import * as FormatVersion from '../format-version-enum.ts';
import type {IndexDefinitions} from '../index-defs.ts';
import type {ClientID} from '../sync/ids.ts';
import {withWrite} from '../with-transactions.ts';
import {
  type ClientMap,
  type ClientV5,
  type ClientV6,
  getClients,
  initClientV6,
  setClients,
} from './clients.ts';

type FormatVersion = Enum<typeof FormatVersion>;

export function setClientsForTesting(
  clients: ClientMap,
  dagStore: Store,
): Promise<ClientMap> {
  return withWrite(dagStore, async dagWrite => {
    await setClients(clients, dagWrite);
    return clients;
  });
}

type PartialClientV5 = Partial<ClientV5> &
  Pick<ClientV5, 'heartbeatTimestampMs' | 'headHash'>;

type PartialClientV6 = Partial<ClientV6> &
  Pick<ClientV6, 'heartbeatTimestampMs' | 'refreshHashes'>;

export function makeClientV5(partialClient: PartialClientV5): ClientV5 {
  return {
    clientGroupID: partialClient.clientGroupID ?? 'make-client-group-id',
    headHash: partialClient.headHash,
    heartbeatTimestampMs: partialClient.heartbeatTimestampMs,
    tempRefreshHash: partialClient.tempRefreshHash ?? null,
  };
}

export function makeClientV6(partialClient: PartialClientV6): ClientV6 {
  return {
    clientGroupID: partialClient.clientGroupID ?? 'make-client-group-id',
    refreshHashes: partialClient.refreshHashes,
    heartbeatTimestampMs: partialClient.heartbeatTimestampMs,
    persistHash: partialClient.persistHash ?? null,
  };
}

export function makeClientMap(
  obj: Record<ClientID, PartialClientV5>,
): ClientMap {
  return new Map(
    Object.entries(obj).map(
      ([id, client]) => [id, makeClientV5(client)] as const,
    ),
  );
}

export async function deleteClientForTesting(
  clientID: ClientID,
  dagStore: Store,
): Promise<void> {
  await withWrite(dagStore, async dagWrite => {
    const clients = new Map(await getClients(dagWrite));
    clients.delete(clientID);
    await setClients(clients, dagWrite);
  });
}

export async function initClientWithClientID(
  clientID: ClientID,
  dagStore: Store,
  mutatorNames: string[],
  indexes: IndexDefinitions,
  formatVersion: FormatVersion,
): Promise<void> {
  assert(formatVersion >= FormatVersion.DD31);
  await initClientV6(
    clientID,
    new LogContext(),
    dagStore,
    mutatorNames,
    indexes,
    formatVersion,
    true,
  );
}
