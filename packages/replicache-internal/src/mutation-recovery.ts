import type {LogContext} from '@rocicorp/logger';
import {
  isClientStateNotFoundResponse,
  Puller,
  PullerDD31,
  PullResponseDD31,
  PullResponseOK,
  PullResponseSDD,
} from './puller.js';
import * as dag from './dag/mod.js';
import * as db from './db/mod.js';
import * as persist from './persist/mod.js';
import * as sync from './sync/mod.js';
import {assertHash} from './hash.js';
import {assert, assertNotUndefined} from './asserts.js';
import type {HTTPRequestInfo} from './http-request-info.js';
import {
  MaybePromise,
  REPLICACHE_FORMAT_VERSION_DD31,
  REPLICACHE_FORMAT_VERSION_SDD,
} from './replicache.js';
import {IDBStore} from './kv/idb-store.js';
import {assertClientSDD, setClients} from './persist/clients.js';
import type {ClientID} from './sync/client-id.js';
import type {Pusher} from './pusher.js';
import type {ClientGroupID} from './sync/client-group-id.js';
import {PUSH_VERSION_DD31, PUSH_VERSION_SDD} from './sync/push.js';

const MUTATION_RECOVERY_LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT = 10 * 2 ** 20; // 10 MB

interface ReplicacheDelegate {
  auth: string;
  clientID: Promise<ClientID>;
  closed: boolean;
  idbName: string;
  name: string;
  online: boolean;
  profileID: Promise<string>;
  puller: Puller;
  pullURL: string;
  pusher: Pusher;
  pushURL: string;
}

interface MutationRecoveryOptions {
  delegate: ReplicacheDelegate;
  readonly wrapInOnlineCheck: (
    f: () => Promise<boolean>,
    name: string,
  ) => Promise<boolean>;
  readonly wrapInReauthRetries: <R>(
    f: (
      requestID: string,
      requestLc: LogContext,
    ) => Promise<{
      httpRequestInfo: HTTPRequestInfo | undefined;
      result: R;
    }>,
    verb: string,
    serverURL: string,
    lc: LogContext,
    preAuth?: () => MaybePromise<void>,
    postAuth?: () => MaybePromise<void>,
  ) => Promise<{
    result: R;
    authFailure: boolean;
  }>;
  readonly isPushDisabled: () => boolean;
  readonly isPullDisabled: () => boolean;
  readonly lc: LogContext;
  readonly enableMutationRecovery: boolean;
  readonly clientGroupIDPromise: Promise<ClientGroupID | undefined>;
}

export class MutationRecovery {
  private _recoveringMutations = false;
  private readonly _options: MutationRecoveryOptions;

  constructor(options: MutationRecoveryOptions) {
    this._options = options;
  }

  async recoverMutations(
    preReadClientMap: persist.ClientMap | undefined,
    ready: Promise<unknown>,
    perdag: dag.Store,
    idbDatabase: persist.IndexedDBDatabase,
    idbDatabases: persist.IDBDatabasesStore,
  ): Promise<boolean> {
    const {lc, enableMutationRecovery, isPushDisabled, delegate} =
      this._options;

    if (
      !enableMutationRecovery ||
      this._recoveringMutations ||
      !delegate.online ||
      delegate.closed ||
      isPushDisabled()
    ) {
      return false;
    }
    const stepDescription = 'Recovering mutations.';
    lc.debug?.('Start:', stepDescription);
    try {
      this._recoveringMutations = true;
      await ready;
      await recoverMutationsFromPerdag(
        idbDatabase,
        this._options,
        perdag,
        preReadClientMap,
      );
      for (const database of Object.values(await idbDatabases.getDatabases())) {
        if (delegate.closed) {
          lc.debug?.('Exiting early due to close:', stepDescription);
          return true;
        }
        if (
          database.replicacheName === delegate.name &&
          database.name !== delegate.idbName
        ) {
          switch (database.replicacheFormatVersion) {
            case REPLICACHE_FORMAT_VERSION_SDD:
            case REPLICACHE_FORMAT_VERSION_DD31:
              await recoverMutationsWithNewPerdag(
                database,
                this._options,
                undefined,
              );
          }
        }
      }
    } catch (e) {
      logMutationRecoveryError(e, lc, stepDescription, delegate);
    } finally {
      lc.debug?.('End:', stepDescription);
      this._recoveringMutations = false;
    }
    return true;
  }
}

function logMutationRecoveryError(
  e: unknown,
  lc: LogContext,
  stepDescription: string,
  closedDelegate: {closed: boolean},
) {
  if (closedDelegate.closed) {
    lc.debug?.(
      `Mutation recovery error likely due to close during:\n${stepDescription}\nError:\n`,
      e,
    );
  } else {
    lc.error?.(
      `Mutation recovery error during:\n${stepDescription}\nError:\n`,
      e,
    );
  }
}

/**
 * @returns When mutations are recovered the resulting updated client map.
 *   Otherwise undefined, which can be because there were no mutations to
 *   recover, or because an error occurred when trying to recover the
 *   mutations.
 */
async function recoverMutationsOfClientSDD(
  client: persist.Client,
  clientID: sync.ClientID,
  perdag: dag.Store,
  database: persist.IndexedDBDatabase,
  options: MutationRecoveryOptions,
): Promise<persist.ClientMap | undefined> {
  assert(database.replicacheFormatVersion === REPLICACHE_FORMAT_VERSION_SDD);
  assertClientSDD(client);

  const {
    delegate,
    lc,
    wrapInOnlineCheck,
    wrapInReauthRetries,
    isPushDisabled,
    isPullDisabled,
  } = options;
  const selfClientID = await delegate.clientID;
  if (selfClientID === clientID) {
    return;
  }
  if (client.lastServerAckdMutationID >= client.mutationID) {
    return;
  }
  const stepDescription = `Recovering mutations for ${clientID}.`;
  lc.debug?.('Start:', stepDescription);
  const dagForOtherClient = new dag.LazyStore(
    perdag,
    MUTATION_RECOVERY_LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT,
    dag.throwChunkHasher,
    assertHash,
  );
  try {
    await dagForOtherClient.withWrite(async write => {
      await write.setHead(db.DEFAULT_HEAD_NAME, client.headHash);
      await write.commit();
    });

    if (isPushDisabled()) {
      lc.debug?.(
        `Cannot recover mutations for client ${clientID} because push is disabled.`,
      );
      return;
    }
    const {pusher, pushURL} = delegate;

    const pushDescription = 'recoveringMutationsPush';
    const pushSucceeded = await wrapInOnlineCheck(async () => {
      const {result: pushResponse} = await wrapInReauthRetries(
        async (requestID: string, requestLc: LogContext) => {
          assertNotUndefined(dagForOtherClient);
          const pushResponse = await sync.push(
            requestID,
            dagForOtherClient,
            requestLc,
            await delegate.profileID,
            undefined,
            clientID,
            pusher,
            pushURL,
            delegate.auth,
            database.schemaVersion,
            PUSH_VERSION_SDD,
          );
          return {result: pushResponse, httpRequestInfo: pushResponse};
        },
        pushDescription,
        delegate.pushURL,
        lc,
      );
      return !!pushResponse && pushResponse.httpStatusCode === 200;
    }, pushDescription);
    if (!pushSucceeded) {
      lc.debug?.(
        `Failed to recover mutations for client ${clientID} due to a push error.`,
      );
      return;
    }

    if (isPullDisabled()) {
      lc.debug?.(
        `Cannot confirm mutations were recovered for client ${clientID} ` +
          `because pull is disabled.`,
      );
      return;
    }
    const {puller, pullURL} = delegate;

    const pullDescription = 'recoveringMutationsPull';
    let pullResponse: PullResponseSDD | undefined;
    const pullSucceeded = await wrapInOnlineCheck(async () => {
      const {result: beginPullResponse} = await wrapInReauthRetries(
        async (requestID: string, requestLc: LogContext) => {
          const beginPullRequest = {
            pullAuth: delegate.auth,
            pullURL,
            schemaVersion: database.schemaVersion,
            puller,
          };
          const beginPullResponse = await sync.beginPullSDD(
            await delegate.profileID,
            clientID,
            beginPullRequest,
            beginPullRequest.puller,
            requestID,
            dagForOtherClient,
            requestLc,
            false,
          );
          return {
            result: beginPullResponse,
            httpRequestInfo: beginPullResponse.httpRequestInfo,
          };
        },
        pullDescription,
        delegate.pullURL,
        lc,
      );
      ({pullResponse} = beginPullResponse);
      return (
        !!pullResponse &&
        beginPullResponse.httpRequestInfo.httpStatusCode === 200
      );
    }, pullDescription);
    if (!pullSucceeded) {
      lc.debug?.(
        `Failed to recover mutations for client ${clientID} due to a pull error.`,
      );
      return;
    }

    if (lc.debug && pullResponse) {
      if (isClientStateNotFoundResponse(pullResponse)) {
        lc.debug?.(
          `Client ${selfClientID} cannot recover mutations for client ` +
            `${clientID}. The client no longer exists on the server.`,
        );
      } else {
        lc.debug?.(
          `Client ${selfClientID} recovered mutations for client ` +
            `${clientID}.  Details`,
          {
            mutationID: client.mutationID,
            lastServerAckdMutationID: client.lastServerAckdMutationID,
            lastMutationID: pullResponse.lastMutationID,
          },
        );
      }
    }

    return await perdag.withWrite(async dagWrite => {
      const clients = await persist.getClients(dagWrite);
      const clientToUpdate = clients.get(clientID);
      if (!clientToUpdate) {
        return clients;
      }

      assertClientSDD(clientToUpdate);

      const setNewClients = async (newClients: persist.ClientMap) => {
        await setClients(newClients, dagWrite);
        await dagWrite.commit();
        return newClients;
      };

      if (isClientStateNotFoundResponse(pullResponse)) {
        const newClients = new Map(clients);
        newClients.delete(clientID);
        return await setNewClients(newClients);
      }

      assert(pullResponse);
      if (
        clientToUpdate.lastServerAckdMutationID >= pullResponse.lastMutationID
      ) {
        return clients;
      }

      const newClients = new Map(clients).set(clientID, {
        ...clientToUpdate,
        lastServerAckdMutationID: (pullResponse as PullResponseOK)
          .lastMutationID,
      });
      return await setNewClients(newClients);
    });
  } catch (e) {
    logMutationRecoveryError(e, lc, stepDescription, delegate);
  } finally {
    await dagForOtherClient.close();
    lc.debug?.('End:', stepDescription);
  }
  return;
}

async function recoverMutationsWithNewPerdag(
  database: persist.IndexedDBDatabase,
  options: MutationRecoveryOptions,
  preReadClientMap: persist.ClientMap | undefined,
) {
  const perKvStore = new IDBStore(database.name);
  const perdag = new dag.StoreImpl(perKvStore, dag.uuidChunkHasher, assertHash);
  try {
    await recoverMutationsFromPerdag(
      database,
      options,
      perdag,
      preReadClientMap,
    );
  } finally {
    await perdag.close();
  }
}

function recoverMutationsFromPerdag(
  database: persist.IndexedDBDatabase,
  options: MutationRecoveryOptions,
  perdag: dag.Store,
  preReadClientMap: persist.ClientMap | undefined,
): Promise<void> {
  if (
    DD31 &&
    database.replicacheFormatVersion === REPLICACHE_FORMAT_VERSION_DD31
  ) {
    return recoverMutationsFromPerdagDD31(database, options, perdag);
  }
  return recoverMutationsFromPerdagSDD(
    database,
    options,
    perdag,
    preReadClientMap,
  );
}

async function recoverMutationsFromPerdagSDD(
  database: persist.IndexedDBDatabase,
  options: MutationRecoveryOptions,
  perdag: dag.Store,
  preReadClientMap: persist.ClientMap | undefined,
): Promise<void> {
  const {delegate, lc} = options;
  const stepDescription = `Recovering mutations from db ${database.name}.`;
  lc.debug?.('Start:', stepDescription);
  try {
    let clientMap: persist.ClientMap | undefined =
      preReadClientMap ||
      (await perdag.withRead(read => persist.getClients(read)));
    const clientIDsVisited = new Set<sync.ClientID>();
    while (clientMap) {
      let newClientMap: persist.ClientMap | undefined;
      for (const [clientID, client] of clientMap) {
        if (delegate.closed) {
          lc.debug?.('Exiting early due to close:', stepDescription);
          return;
        }
        if (!clientIDsVisited.has(clientID)) {
          clientIDsVisited.add(clientID);
          newClientMap = await recoverMutationsOfClientSDD(
            client,
            clientID,
            perdag,
            database,
            options,
          );
          if (newClientMap) {
            break;
          }
        }
      }
      clientMap = newClientMap;
    }
  } catch (e) {
    logMutationRecoveryError(e, lc, stepDescription, delegate);
  }
  lc.debug?.('End:', stepDescription);
}

async function recoverMutationsFromPerdagDD31(
  database: persist.IndexedDBDatabase,
  options: MutationRecoveryOptions,
  perdag: dag.Store,
): Promise<void> {
  const {delegate, lc} = options;
  const stepDescription = `Recovering mutations from db ${database.name}.`;
  lc.debug?.('Start:', stepDescription);
  try {
    let clientGroups: persist.ClientGroupMap | undefined =
      await perdag.withRead(read => persist.getClientGroups(read));
    const clientGroupIDsVisited = new Set<sync.ClientGroupID>();
    while (clientGroups) {
      let newClientGroups: persist.ClientGroupMap | undefined;
      for (const [clientGroupID, clientGroup] of clientGroups) {
        if (delegate.closed) {
          lc.debug?.('Exiting early due to close:', stepDescription);
          return;
        }
        if (!clientGroupIDsVisited.has(clientGroupID)) {
          clientGroupIDsVisited.add(clientGroupID);
          newClientGroups = await recoverMutationsOfClientGroupDD31(
            clientGroup,
            clientGroupID,
            perdag,
            database,
            options,
          );
          if (newClientGroups) {
            break;
          }
        }
      }
      clientGroups = newClientGroups;
    }
  } catch (e) {
    logMutationRecoveryError(e, lc, stepDescription, delegate);
  }
  lc.debug?.('End:', stepDescription);
}

/**
 * @returns When mutations are recovered the resulting updated client group map.
 *   Otherwise undefined, which can be because there were no mutations to
 *   recover, or because an error occurred when trying to recover the mutations.
 */
async function recoverMutationsOfClientGroupDD31(
  clientGroup: persist.ClientGroup,
  clientGroupID: sync.ClientGroupID,
  perdag: dag.Store,
  database: persist.IndexedDBDatabase,
  options: MutationRecoveryOptions,
): Promise<persist.ClientGroupMap | undefined> {
  assert(DD31);
  assert(database.replicacheFormatVersion === REPLICACHE_FORMAT_VERSION_DD31);

  const {
    delegate,
    lc,
    wrapInOnlineCheck,
    wrapInReauthRetries,
    isPushDisabled,
    isPullDisabled,
  } = options;
  const selfClientGroupID = await options.clientGroupIDPromise;
  if (selfClientGroupID === clientGroupID) {
    return;
  }

  let clientID: sync.ClientID | undefined;

  // If all local mutations have been applied then exit.
  let allAckd = true;
  for (const [cid, mutationID] of Object.entries(clientGroup.mutationIDs)) {
    // if not present then the server has not acknowledged this client's mutations.
    if (
      !clientGroup.lastServerAckdMutationIDs[cid] ||
      clientGroup.lastServerAckdMutationIDs[cid] < mutationID
    ) {
      clientID = cid;
      allAckd = false;
      break;
    }
  }
  if (allAckd) {
    return;
  }

  const stepDescription = `Recovering mutations for client group ${clientGroupID}.`;
  lc.debug?.('Start:', stepDescription);
  const dagForOtherClientGroup = new dag.LazyStore(
    perdag,
    MUTATION_RECOVERY_LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT,
    dag.throwChunkHasher,
    assertHash,
  );
  try {
    await dagForOtherClientGroup.withWrite(async write => {
      await write.setHead(db.DEFAULT_HEAD_NAME, clientGroup.headHash);
      await write.commit();
    });

    if (isPushDisabled()) {
      lc.debug?.(
        `Cannot recover mutations for client group ${clientGroupID} because push is disabled.`,
      );
      return;
    }

    // TODO(DD31) pusher is a really a PusherDD31... Signal that somehow.
    const {pusher, pushURL} = delegate;

    const pushDescription = 'recoveringMutationsPush';
    const pushSucceeded = await wrapInOnlineCheck(async () => {
      const {result: pushResponse} = await wrapInReauthRetries(
        async (requestID: string, requestLc: LogContext) => {
          assert(clientID);
          assert(dagForOtherClientGroup);
          const pushResponse = await sync.push(
            requestID,
            dagForOtherClientGroup,
            requestLc,
            await delegate.profileID,
            clientGroupID,
            // TODO(DD31): clientID is not needed in DD31. It is currently kept for debugging purpose.
            clientID,
            pusher,
            pushURL,
            delegate.auth,
            database.schemaVersion,
            PUSH_VERSION_DD31,
          );
          return {result: pushResponse, httpRequestInfo: pushResponse};
        },
        pushDescription,
        delegate.pushURL,
        lc,
      );
      return !!pushResponse && pushResponse.httpStatusCode === 200;
    }, pushDescription);
    if (!pushSucceeded) {
      lc.debug?.(
        `Failed to recover mutations for client ${clientGroupID} due to a push error.`,
      );
      return;
    }

    if (isPullDisabled()) {
      lc.debug?.(
        `Cannot confirm mutations were recovered for client ${clientGroupID} ` +
          `because pull is disabled.`,
      );
      return;
    }
    // TODO(DD31) puller is a really a PullerDD31... Signal that somehow.
    const {puller, pullURL} = delegate;

    const pullDescription = 'recoveringMutationsPull';
    let pullResponse: PullResponseDD31 | undefined;
    const pullSucceeded = await wrapInOnlineCheck(async () => {
      const {result: beginPullResponse} = await wrapInReauthRetries(
        async (requestID: string, requestLc: LogContext) => {
          const beginPullRequest = {
            pullAuth: delegate.auth,
            pullURL,
            schemaVersion: database.schemaVersion,
            puller,
          };
          assert(clientID);
          const beginPullResponse = await sync.beginPullDD31(
            await delegate.profileID,
            clientID,
            clientGroupID,
            beginPullRequest,
            // TODO(DD31): Figure this out.
            beginPullRequest.puller as PullerDD31,
            requestID,
            dagForOtherClientGroup,
            requestLc,
            false,
          );
          return {
            result: beginPullResponse,
            httpRequestInfo: beginPullResponse.httpRequestInfo,
          };
        },
        pullDescription,
        delegate.pullURL,
        lc,
      );
      ({pullResponse} = beginPullResponse);
      return (
        !!pullResponse &&
        beginPullResponse.httpRequestInfo.httpStatusCode === 200
      );
    }, pullDescription);
    if (!pullSucceeded) {
      lc.debug?.(
        `Failed to recover mutations for client ${clientGroupID} due to a pull error.`,
      );
      return;
    }

    // TODO(arv): Refactor to make pullResponse a const.
    // pullResponse must be non undefined because pullSucceeded is true.
    assert(pullResponse);
    if (lc.debug) {
      if (isClientStateNotFoundResponse(pullResponse)) {
        lc.debug?.(
          `Client group ${selfClientGroupID} cannot recover mutations for client group ${clientGroupID}. The client group no longer exists on the server.`,
        );
      } else {
        lc.debug?.(
          `Client group ${selfClientGroupID} recovered mutations for client group ${clientGroupID}.  Details`,
          {
            mutationIDs: clientGroup.mutationIDs,
            lastServerAckdMutationIDs: clientGroup.lastServerAckdMutationIDs,
            lastMutationIDChanges: pullResponse.lastMutationIDChanges,
          },
        );
      }
    }

    return await perdag.withWrite(async dagWrite => {
      const clientGroups = await persist.getClientGroups(dagWrite);
      const clientGroupToUpdate = clientGroups.get(clientGroupID);
      if (!clientGroupToUpdate) {
        return clientGroups;
      }

      const setNewClientGroups = async (
        newClientGroups: persist.ClientGroupMap,
      ) => {
        await persist.setClientGroups(newClientGroups, dagWrite);
        await dagWrite.commit();
        return newClientGroups;
      };

      if (isClientStateNotFoundResponse(pullResponse)) {
        const newClientGroups = new Map(clientGroups);
        newClientGroups.delete(clientGroupID);
        return await setNewClientGroups(newClientGroups);
      }

      assert(pullResponse);
      const lastServerAckdMutationIDsUpdates: Record<ClientID, number> = {};
      let anyMutationIDsUpdated = false;
      for (const [clientID, lastMutationIDChange] of Object.entries(
        pullResponse.lastMutationIDChanges,
      )) {
        if (
          clientGroupToUpdate.lastServerAckdMutationIDs[clientID] <
          lastMutationIDChange
        ) {
          lastServerAckdMutationIDsUpdates[clientID] = lastMutationIDChange;
          anyMutationIDsUpdated = true;
        }
      }
      if (!anyMutationIDsUpdated) {
        return clientGroups;
      }

      const newClientGroups = new Map(clientGroups).set(clientGroupID, {
        ...clientGroupToUpdate,
        lastServerAckdMutationIDs: {
          ...clientGroupToUpdate.lastServerAckdMutationIDs,
          ...lastServerAckdMutationIDsUpdates,
        },
      });
      return await setNewClientGroups(newClientGroups);
    });
  } catch (e) {
    logMutationRecoveryError(e, lc, stepDescription, delegate);
  } finally {
    await dagForOtherClientGroup.close();
    lc.debug?.('End:', stepDescription);
  }
  return;
}
