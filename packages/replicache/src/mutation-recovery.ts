import type {LogContext} from '@rocicorp/logger';
import {assert, assertNotUndefined} from 'shared/src/asserts.js';
import {throwChunkHasher, uuidChunkHasher} from './dag/chunk.js';
import {LazyStore} from './dag/lazy-store.js';
import {StoreImpl} from './dag/store-impl.js';
import type {Store} from './dag/store.js';
import {DEFAULT_HEAD_NAME} from './db/commit.js';
import {
  ClientStateNotFoundResponse,
  VersionNotSupportedResponse,
  isClientStateNotFoundResponse,
  isVersionNotSupportedResponse,
} from './error-responses.js';
import {
  FormatVersion,
  parseReplicacheFormatVersion as parseFormatVersion,
} from './format-version.js';
import {assertHash} from './hash.js';
import type {HTTPRequestInfo} from './http-request-info.js';
import type {CreateStore} from './kv/store.js';
import {
  ClientGroup,
  ClientGroupMap,
  getClientGroups,
  disableClientGroup as persistDisableClientGroup,
  setClientGroups,
} from './persist/client-groups.js';
import {
  Client,
  ClientMap,
  assertClientV4,
  getClients,
  setClients,
} from './persist/clients.js';
import type {
  IDBDatabasesStore,
  IndexedDBDatabase,
} from './persist/idb-databases-store.js';
import type {
  PullResponseOKV1,
  PullResponseV0,
  PullResponseV1,
  Puller,
} from './puller.js';
import type {PushResponse, Pusher} from './pusher.js';
import type {ClientGroupID, ClientID} from './sync/ids.js';
import {beginPullV0, beginPullV1} from './sync/pull.js';
import {PUSH_VERSION_DD31, PUSH_VERSION_SDD, push} from './sync/push.js';
import type {MaybePromise} from 'shared/src/types.js';
import {withRead, withWrite} from './with-transactions.js';

const MUTATION_RECOVERY_LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT = 10 * 2 ** 20; // 10 MB

interface ReplicacheDelegate {
  clientID: ClientID;
  closed: boolean;
  idbName: string;
  name: string;
  online: boolean;
  profileID: Promise<string>;
  puller: Puller;
  pusher: Pusher;
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
  #recoveringMutations = false;
  readonly #options: MutationRecoveryOptions;

  constructor(options: MutationRecoveryOptions) {
    this.#options = options;
  }

  async recoverMutations(
    preReadClientMap: ClientMap | undefined,
    ready: Promise<unknown>,
    perdag: Store,
    idbDatabase: IndexedDBDatabase,
    idbDatabases: IDBDatabasesStore,
    createStore: CreateStore,
  ): Promise<boolean> {
    const {lc, enableMutationRecovery, isPushDisabled, delegate} =
      this.#options;

    if (
      !enableMutationRecovery ||
      this.#recoveringMutations ||
      !delegate.online ||
      delegate.closed ||
      isPushDisabled()
    ) {
      return false;
    }
    const stepDescription = 'Recovering mutations.';
    lc.debug?.('Start:', stepDescription);
    try {
      this.#recoveringMutations = true;
      await ready;
      await recoverMutationsFromPerdag(
        idbDatabase,
        this.#options,
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
            case FormatVersion.SDD:
            case FormatVersion.DD31:
            case FormatVersion.V6:
            case FormatVersion.V7:
              await recoverMutationsWithNewPerdag(
                database,
                this.#options,
                undefined,
                createStore,
              );
          }
        }
      }
    } catch (e) {
      logMutationRecoveryError(e, lc, stepDescription, delegate);
    } finally {
      lc.debug?.('End:', stepDescription);
      this.#recoveringMutations = false;
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
async function recoverMutationsOfClientV4(
  client: Client,
  clientID: ClientID,
  perdag: Store,
  database: IndexedDBDatabase,
  options: MutationRecoveryOptions,
  formatVersion: FormatVersion,
): Promise<ClientMap | undefined> {
  assert(database.replicacheFormatVersion === FormatVersion.SDD);
  assertClientV4(client);

  const {
    delegate,
    lc,
    wrapInOnlineCheck,
    wrapInReauthRetries,
    isPushDisabled,
    isPullDisabled,
  } = options;
  const selfClientID = delegate.clientID;
  if (selfClientID === clientID) {
    return;
  }
  if (client.lastServerAckdMutationID >= client.mutationID) {
    return;
  }
  const stepDescription = `Recovering mutations for ${clientID}.`;
  lc.debug?.('Start:', stepDescription);
  const lazyDagForOtherClient = new LazyStore(
    perdag,
    MUTATION_RECOVERY_LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT,
    throwChunkHasher,
    assertHash,
  );
  try {
    await withWrite(lazyDagForOtherClient, write =>
      write.setHead(DEFAULT_HEAD_NAME, client.headHash),
    );

    if (isPushDisabled()) {
      lc.debug?.(
        `Cannot recover mutations for client ${clientID} because push is disabled.`,
      );
      return;
    }
    const {pusher} = delegate;

    const pushDescription = 'recoveringMutationsPush';
    const pushSucceeded = await wrapInOnlineCheck(async () => {
      const {result: pusherResult} = await wrapInReauthRetries(
        async (requestID: string, requestLc: LogContext) => {
          assertNotUndefined(lazyDagForOtherClient);
          const pusherResult = await push(
            requestID,
            lazyDagForOtherClient,
            requestLc,
            await delegate.profileID,
            undefined,
            clientID,
            pusher,
            database.schemaVersion,
            PUSH_VERSION_SDD,
          );
          return {
            result: pusherResult,
            httpRequestInfo: pusherResult?.httpRequestInfo,
          };
        },
        pushDescription,
        lc,
      );

      return (
        !!pusherResult && pusherResult.httpRequestInfo.httpStatusCode === 200
      );
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
    const {puller} = delegate;

    const pullDescription = 'recoveringMutationsPull';
    let pullResponse: PullResponseV0 | undefined;
    const pullSucceeded = await wrapInOnlineCheck(async () => {
      const {result: beginPullResponse} = await wrapInReauthRetries(
        async (requestID: string, requestLc: LogContext) => {
          const beginPullResponse = await beginPullV0(
            await delegate.profileID,
            clientID,
            database.schemaVersion,
            puller,
            requestID,
            lazyDagForOtherClient,
            formatVersion,
            requestLc,
            false,
          );
          return {
            result: beginPullResponse,
            httpRequestInfo: beginPullResponse.httpRequestInfo,
          };
        },
        pullDescription,
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
      } else if (isVersionNotSupportedResponse(pullResponse)) {
        lc.debug?.(
          `Version is not supported on the server. versionType: ${pullResponse.versionType}. Cannot recover mutations for client ${clientID}.`,
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

    return await withWrite(perdag, async dagWrite => {
      const clients = await getClients(dagWrite);
      const clientToUpdate = clients.get(clientID);
      if (!clientToUpdate) {
        return clients;
      }

      assertClientV4(clientToUpdate);

      const setNewClients = async (newClients: ClientMap) => {
        await setClients(newClients, dagWrite);
        return newClients;
      };

      if (
        isClientStateNotFoundResponse(pullResponse) ||
        // Even though SDD did not have VersionNotSupported we can still get
        // this if the server was upgraded to handle this. It seems better to
        // delete the client at this point.
        isVersionNotSupportedResponse(pullResponse)
      ) {
        const newClients = new Map(clients);
        newClients.delete(clientID);
        return setNewClients(newClients);
      }

      assert(pullResponse);
      if (
        clientToUpdate.lastServerAckdMutationID >= pullResponse.lastMutationID
      ) {
        return clients;
      }

      const newClients = new Map(clients).set(clientID, {
        ...clientToUpdate,
        lastServerAckdMutationID: pullResponse.lastMutationID,
      });
      return setNewClients(newClients);
    });
  } catch (e) {
    logMutationRecoveryError(e, lc, stepDescription, delegate);
  } finally {
    await lazyDagForOtherClient.close();
    lc.debug?.('End:', stepDescription);
  }
  return;
}

async function recoverMutationsWithNewPerdag(
  database: IndexedDBDatabase,
  options: MutationRecoveryOptions,
  preReadClientMap: ClientMap | undefined,
  createStore: CreateStore,
) {
  const perKvStore = createStore(database.name);
  const perdag = new StoreImpl(perKvStore, uuidChunkHasher, assertHash);
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
  database: IndexedDBDatabase,
  options: MutationRecoveryOptions,
  perdag: Store,
  preReadClientMap: ClientMap | undefined,
): Promise<void> {
  if (database.replicacheFormatVersion >= FormatVersion.DD31) {
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
  database: IndexedDBDatabase,
  options: MutationRecoveryOptions,
  perdag: Store,
  preReadClientMap: ClientMap | undefined,
): Promise<void> {
  const {delegate, lc} = options;
  const stepDescription = `Recovering mutations from db ${database.name}.`;
  lc.debug?.('Start:', stepDescription);
  try {
    const formatVersion = parseFormatVersion(database.replicacheFormatVersion);
    let clientMap: ClientMap | undefined =
      preReadClientMap || (await withRead(perdag, read => getClients(read)));
    const clientIDsVisited = new Set<ClientID>();
    while (clientMap) {
      let newClientMap: ClientMap | undefined;
      for (const [clientID, client] of clientMap) {
        if (delegate.closed) {
          lc.debug?.('Exiting early due to close:', stepDescription);
          return;
        }
        if (!clientIDsVisited.has(clientID)) {
          clientIDsVisited.add(clientID);
          newClientMap = await recoverMutationsOfClientV4(
            client,
            clientID,
            perdag,
            database,
            options,
            formatVersion,
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
  database: IndexedDBDatabase,
  options: MutationRecoveryOptions,
  perdag: Store,
): Promise<void> {
  const {delegate, lc} = options;
  const stepDescription = `Recovering mutations from db ${database.name}.`;
  lc.debug?.('Start:', stepDescription);
  try {
    const formatVersion = parseFormatVersion(database.replicacheFormatVersion);
    let clientGroups: ClientGroupMap | undefined = await withRead(
      perdag,
      read => getClientGroups(read),
    );
    const clientGroupIDsVisited = new Set<ClientGroupID>();
    while (clientGroups) {
      let newClientGroups: ClientGroupMap | undefined;
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
            formatVersion,
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

function isResponseThatShouldDisableClientGroup(
  response: PushResponse | PullResponseV1 | undefined,
): response is ClientStateNotFoundResponse | VersionNotSupportedResponse {
  return (
    isClientStateNotFoundResponse(response) ||
    isVersionNotSupportedResponse(response)
  );
}

async function disableClientGroup(
  lc: LogContext,
  selfClientGroupID: string,
  clientGroupID: string,
  response: ClientStateNotFoundResponse | VersionNotSupportedResponse,
  perdag: Store,
) {
  if (isClientStateNotFoundResponse(response)) {
    lc.debug?.(
      `Client group ${selfClientGroupID} cannot recover mutations for client group ${clientGroupID}. The client group is unknown on the server. Marking it as disabled.`,
    );
  } else if (isVersionNotSupportedResponse(response)) {
    lc.debug?.(
      `Client group ${selfClientGroupID} cannot recover mutations for client group ${clientGroupID}. The client group's version is not supported on the server. versionType: ${response.versionType}. Marking it as disabled.`,
    );
  }
  // The client group is not the main client group so we do not need the
  // Replicache instance to update its internal _isClientGroupDisabled
  // property.
  await withWrite(perdag, perdagWrite =>
    persistDisableClientGroup(clientGroupID, perdagWrite),
  );
}

/**
 * @returns When mutations are recovered the resulting updated client group map.
 *   Otherwise undefined, which can be because there were no mutations to
 *   recover, or because an error occurred when trying to recover the mutations.
 */
async function recoverMutationsOfClientGroupDD31(
  clientGroup: ClientGroup,
  clientGroupID: ClientGroupID,
  perdag: Store,
  database: IndexedDBDatabase,
  options: MutationRecoveryOptions,
  formatVersion: FormatVersion,
): Promise<ClientGroupMap | undefined> {
  assert(database.replicacheFormatVersion >= FormatVersion.DD31);

  const {
    delegate,
    lc,
    wrapInOnlineCheck,
    wrapInReauthRetries,
    isPushDisabled,
    isPullDisabled,
  } = options;

  const selfClientGroupID = await options.clientGroupIDPromise;
  assertNotUndefined(selfClientGroupID);
  if (selfClientGroupID === clientGroupID) {
    return;
  }

  let clientID: ClientID | undefined;

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

  if (clientGroup.disabled) {
    lc.debug?.(
      `Not recovering mutations for client group ${clientGroupID} because group is disabled.`,
    );
    return;
  }

  const stepDescription = `Recovering mutations for client group ${clientGroupID}.`;
  lc.debug?.('Start:', stepDescription);
  const lazyDagForOtherClientGroup = new LazyStore(
    perdag,
    MUTATION_RECOVERY_LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT,
    throwChunkHasher,
    assertHash,
  );
  try {
    await withWrite(lazyDagForOtherClientGroup, write =>
      write.setHead(DEFAULT_HEAD_NAME, clientGroup.headHash),
    );

    if (isPushDisabled()) {
      lc.debug?.(
        `Cannot recover mutations for client group ${clientGroupID} because push is disabled.`,
      );
      return;
    }

    const {pusher} = delegate;

    const pushDescription = 'recoveringMutationsPush';
    const pushSucceeded = await wrapInOnlineCheck(async () => {
      const {result: pusherResult} = await wrapInReauthRetries(
        async (requestID: string, requestLc: LogContext) => {
          assert(clientID);
          assert(lazyDagForOtherClientGroup);
          const pusherResult = await push(
            requestID,
            lazyDagForOtherClientGroup,
            requestLc,
            await delegate.profileID,
            clientGroupID,
            // TODO(DD31): clientID is not needed in DD31. It is currently kept for debugging purpose.
            clientID,
            pusher,
            database.schemaVersion,
            PUSH_VERSION_DD31,
          );
          return {
            result: pusherResult,
            httpRequestInfo: pusherResult?.httpRequestInfo,
          };
        },
        pushDescription,
        lc,
      );
      if (!pusherResult) {
        return false;
      }
      const pusherResponse = pusherResult.response;
      if (isResponseThatShouldDisableClientGroup(pusherResponse)) {
        await disableClientGroup(
          lc,
          selfClientGroupID,
          clientGroupID,
          pusherResponse,
          perdag,
        );
        return false;
      }
      return pusherResult.httpRequestInfo.httpStatusCode === 200;
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
    const {puller} = delegate;

    const pullDescription = 'recoveringMutationsPull';
    let okPullResponse: PullResponseOKV1 | undefined;
    const pullSucceeded = await wrapInOnlineCheck(async () => {
      const {result: beginPullResponse} = await wrapInReauthRetries(
        async (requestID: string, requestLc: LogContext) => {
          assert(clientID);
          const beginPullResponse = await beginPullV1(
            await delegate.profileID,
            clientID,
            clientGroupID,
            database.schemaVersion,
            puller,
            requestID,
            lazyDagForOtherClientGroup,
            formatVersion,
            requestLc,
            false,
          );
          return {
            result: beginPullResponse,
            httpRequestInfo: beginPullResponse.httpRequestInfo,
          };
        },
        pullDescription,
        lc,
      );
      const {pullResponse} = beginPullResponse;
      if (isResponseThatShouldDisableClientGroup(pullResponse)) {
        await disableClientGroup(
          lc,
          selfClientGroupID,
          clientGroupID,
          pullResponse,
          perdag,
        );
        return false;
      }
      if (
        !pullResponse ||
        beginPullResponse.httpRequestInfo.httpStatusCode !== 200
      ) {
        return false;
      }
      okPullResponse = pullResponse;
      return true;
    }, pullDescription);
    if (!pullSucceeded) {
      lc.debug?.(
        `Failed to recover mutations for client ${clientGroupID} due to a pull error.`,
      );
      return;
    }

    // TODO(arv): Refactor to make pullResponse a const.
    // pullResponse must be non undefined because pullSucceeded is true.
    assert(okPullResponse);
    lc.debug?.(
      `Client group ${selfClientGroupID} recovered mutations for client group ${clientGroupID}.  Details`,
      {
        mutationIDs: clientGroup.mutationIDs,
        lastServerAckdMutationIDs: clientGroup.lastServerAckdMutationIDs,
        lastMutationIDChanges: okPullResponse.lastMutationIDChanges,
      },
    );

    return await withWrite(perdag, async dagWrite => {
      const clientGroups = await getClientGroups(dagWrite);
      const clientGroupToUpdate = clientGroups.get(clientGroupID);
      if (!clientGroupToUpdate) {
        return clientGroups;
      }

      assert(okPullResponse);
      const lastServerAckdMutationIDsUpdates: Record<ClientID, number> = {};
      let anyMutationIDsUpdated = false;
      for (const [clientID, lastMutationIDChange] of Object.entries(
        okPullResponse.lastMutationIDChanges,
      )) {
        if (
          (clientGroupToUpdate.lastServerAckdMutationIDs[clientID] ?? 0) <
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
      await setClientGroups(newClientGroups, dagWrite);
      return newClientGroups;
    });
  } catch (e) {
    logMutationRecoveryError(e, lc, stepDescription, delegate);
  } finally {
    await lazyDagForOtherClientGroup.close();
    lc.debug?.('End:', stepDescription);
  }
  return;
}
