import type {LogContext} from '@rocicorp/logger';
import {
  isClientStateNotFoundResponse,
  Puller,
  PullResponse,
  PullResponseDD31,
  PullResponseOK,
  PullResponseOKDD31,
} from './puller';
import * as dag from './dag/mod';
import * as db from './db/mod';
import * as persist from './persist/mod';
import * as sync from './sync/mod';
import {assertHash} from './hash';
import {assertNotUndefined} from './asserts';
import type {HTTPRequestInfo} from './http-request-info';
import {MaybePromise, REPLICACHE_FORMAT_VERSION} from './replicache';
import {IDBStore} from './kv/idb-store.js';
import {assertClientSDD, isClientSDD, setClients} from './persist/clients.js';
import type {ClientID} from './sync/client-id.js';
import type {Pusher} from './pusher.js';

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
          database.name === delegate.idbName ||
          database.replicacheName !== delegate.name ||
          // TODO: when REPLICACHE_FORMAT_VERSION is update
          // need to also handle previous REPLICACHE_FORMAT_VERSIONs
          database.replicacheFormatVersion !== REPLICACHE_FORMAT_VERSION
        ) {
          continue;
        }
        await recoverMutationsWithNewPerdag(database, this._options, undefined);
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
async function recoverMutationsOfClient(
  client: persist.Client,
  branchID: sync.BranchID | undefined,
  clientID: sync.ClientID,
  perdag: dag.Store,
  database: persist.IndexedDBDatabase,
  options: MutationRecoveryOptions,
): Promise<persist.ClientMap | undefined> {
  // TODO(DD31): Implement this.
  if (DD31) {
    return;
  }

  if (!isClientSDD(client)) {
    return undefined;
  }

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
    return undefined;
  }
  if (client.lastServerAckdMutationID >= client.mutationID) {
    return undefined;
  }
  const stepDescription = `Recovering mutations for ${clientID}.`;
  lc.debug?.('Start:', stepDescription);
  let dagForOtherClientToClose: dag.LazyStore | undefined;
  try {
    const dagForOtherClient = (dagForOtherClientToClose = new dag.LazyStore(
      perdag,
      MUTATION_RECOVERY_LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT,
      dag.throwChunkHasher,
      assertHash,
    ));

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
            branchID,
            clientID,
            pusher,
            pushURL,
            delegate.auth,
            database.schemaVersion,
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
    let pullResponse: PullResponse | PullResponseDD31 | undefined;
    const pullSucceeded = await wrapInOnlineCheck(async () => {
      const {result: beginPullResponse} = await wrapInReauthRetries(
        async (requestID: string, requestLc: LogContext) => {
          const beginPullRequest = {
            pullAuth: delegate.auth,
            pullURL,
            schemaVersion: database.schemaVersion,
            puller,
          };
          const beginPullResponse = await sync.beginPull(
            await delegate.profileID,
            clientID,
            branchID,
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
          DD31
            ? {
                mutationID: client.mutationID,
                lastServerAckdMutationID: client.lastServerAckdMutationID,
                lastMutationIDChanges: (pullResponse as PullResponseOKDD31)
                  .lastMutationIDChanges,
              }
            : {
                mutationID: client.mutationID,
                lastServerAckdMutationID: client.lastServerAckdMutationID,
                lastMutationID: (pullResponse as PullResponseOK).lastMutationID,
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

      if (
        clientToUpdate.lastServerAckdMutationID >=
        (pullResponse as PullResponseOK).lastMutationID
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
    return;
  } finally {
    await dagForOtherClientToClose?.close();
    lc.debug?.('End:', stepDescription);
  }
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

async function recoverMutationsFromPerdag(
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
          newClientMap = await recoverMutationsOfClient(
            client,
            // TODO(dd31): Iterate over all branch ids...
            DD31 ? 'FAKE_BRANCH_ID_FOR_RECOVER_MUTATION' : undefined,
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
