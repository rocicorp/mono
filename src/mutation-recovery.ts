import type {LogContext} from '@rocicorp/logger';
import type {PullResponse, PullResponseOK} from './puller';
import * as dag from './dag/mod';
import * as db from './db/mod';
import * as persist from './persist/mod';
import * as sync from './sync/mod';
import {assertHash, assertNotTempHash} from './hash';
import {assertNotUndefined} from './asserts';
import type {HTTPRequestInfo} from './http-request-info';
import {
  MaybePromise,
  MutatorDefs,
  REPLICACHE_FORMAT_VERSION,
} from './replicache';
import type {Replicache} from './replicache';
import {IDBStore} from './kv/idb-store.js';
import {isClientStateNotFoundResponse} from './sync/errors';

const MUTATION_RECOVERY_LAZY_STORE_SOURCE_CHUNK_CACHE_SIZE_LIMIT = 10 * 2 ** 20; // 10 MB

interface ReplicacheOptions {
  readonly wrapInOnlineCheck: (
    f: () => Promise<boolean>,
    name: string,
  ) => Promise<boolean>;
  readonly wrapInReauthRetries: <R>(
    f: () => Promise<{
      httpRequestInfo: HTTPRequestInfo | undefined;
      result: R;
    }>,
    verb: string,
    serverURL: string,
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

export class MutationRecovery<M extends MutatorDefs> {
  private readonly _enableMutationRecovery: boolean;
  private readonly _replicache: Replicache<M>;
  private readonly _lc: LogContext;
  private _recoveringMutations = false;
  private readonly _wrapInOnlineCheck: (
    f: () => Promise<boolean>,
    name: string,
  ) => Promise<boolean>;
  private readonly _wrapInReauthRetries: <R>(
    f: () => Promise<{httpRequestInfo: HTTPRequestInfo | undefined; result: R}>,
    verb: string,
    serverURL: string,
    preAuth?: (() => MaybePromise<void>) | undefined,
    postAuth?: (() => MaybePromise<void>) | undefined,
  ) => Promise<{result: R; authFailure: boolean}>;
  private readonly _isPushDisabled: () => boolean;
  private readonly _isPullDisabled: () => boolean;

  constructor(replicache: Replicache<M>, options: ReplicacheOptions) {
    this._replicache = replicache;
    this._enableMutationRecovery = options.enableMutationRecovery;
    this._lc = options.lc;
    this._wrapInOnlineCheck = options.wrapInOnlineCheck;
    this._wrapInReauthRetries = options.wrapInReauthRetries;
    this._isPushDisabled = options.isPushDisabled;
    this._isPullDisabled = options.isPullDisabled;
  }

  async recoverMutations(
    preReadClientMap: persist.ClientMap | undefined,
    ready: Promise<unknown>,
    perdag: dag.Store,
    idbDatabase: persist.IndexedDBDatabase,
    idbDatabases: persist.IDBDatabasesStore,
  ): Promise<boolean> {
    const {_lc: lc} = this;
    const delegate = this._replicache;

    if (
      !this._enableMutationRecovery ||
      this._recoveringMutations ||
      !delegate.online ||
      delegate.closed ||
      this._isPushDisabled()
    ) {
      return false;
    }
    const stepDescription = 'Recovering mutations.';
    lc.debug?.('Start:', stepDescription);
    try {
      this._recoveringMutations = true;
      await ready;
      await this._recoverMutationsFromPerdag(
        idbDatabase,
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
        await this._recoverMutationsFromPerdag(database);
      }
    } catch (e) {
      logMutationRecoveryError(e, lc, stepDescription, delegate);
    } finally {
      lc.debug?.('End:', stepDescription);
      this._recoveringMutations = false;
    }
    return true;
  }

  private async _recoverMutationsFromPerdag(
    database: persist.IndexedDBDatabase,
    perdag?: dag.Store,
    preReadClientMap?: persist.ClientMap,
  ): Promise<void> {
    const {_lc: lc} = this;
    const delegate = this._replicache;
    const stepDescription = `Recovering mutations from db ${database.name}.`;
    lc.debug?.('Start:', stepDescription);
    let perDagToClose: dag.Store | undefined = undefined;
    try {
      if (!perdag) {
        const perKvStore = new IDBStore(database.name);
        perdag = perDagToClose = new dag.StoreImpl(
          perKvStore,
          dag.throwChunkHasher,
          assertNotTempHash,
        );
      }
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
            newClientMap = await this._recoverMutationsOfClient(
              client,
              clientID,
              perdag,
              database,
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
    } finally {
      await perDagToClose?.close();
      lc.debug?.('End:', stepDescription);
    }
  }

  /**
   * @returns When mutations are recovered the resulting updated client map.
   *   Otherwise undefined, which can be because there were no mutations to
   *   recover, or because an error occurred when trying to recover the
   *   mutations.
   */
  private async _recoverMutationsOfClient(
    client: persist.Client,
    clientID: sync.ClientID,
    perdag: dag.Store,
    database: persist.IndexedDBDatabase,
  ) {
    const {_lc: lc} = this;
    const delegate = this._replicache;

    const {
      _wrapInOnlineCheck: wrapInOnlineCheck,
      _wrapInReauthRetries: wrapInReauthRetries,
      _isPushDisabled: isPushDisabled,
      _isPullDisabled: isPullDisabled,
    } = this;
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

      const pushRequestID = sync.newRequestID(clientID);
      const pushDescription = 'recoveringMutationsPush';
      const pushLC = lc
        .addContext(pushDescription)
        .addContext('request_id', pushRequestID);
      const pushSucceeded = await wrapInOnlineCheck(async () => {
        const {result: pushResponse} = await wrapInReauthRetries(
          async () => {
            assertNotUndefined(dagForOtherClient);
            const pushResponse = await sync.push(
              pushRequestID,
              dagForOtherClient,
              pushLC,
              await delegate.profileID,
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

      const requestID = sync.newRequestID(clientID);
      const pullDescription = 'recoveringMutationsPull';
      const pullLC = lc
        .addContext(pullDescription)
        .addContext('request_id', requestID);

      let pullResponse: PullResponse | undefined;
      const pullSucceeded = await wrapInOnlineCheck(async () => {
        const {result: beginPullResponse} = await wrapInReauthRetries(
          async () => {
            const beginPullRequest = {
              pullAuth: delegate.auth,
              pullURL,
              schemaVersion: database.schemaVersion,
              puller,
            };
            const beginPullResponse = await sync.beginPull(
              await delegate.profileID,
              clientID,
              beginPullRequest,
              beginPullRequest.puller,
              requestID,
              dagForOtherClient,
              pullLC,
              false,
            );
            return {
              result: beginPullResponse,
              httpRequestInfo: beginPullResponse.httpRequestInfo,
            };
          },
          pullDescription,
          delegate.pullURL,
        );
        pullResponse = beginPullResponse.pullResponse;
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

      if (pullResponse && isClientStateNotFoundResponse(pullResponse)) {
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
            lastMutationID: pullResponse?.lastMutationID,
          },
        );
      }
      const newClientMap = await persist.updateClients(
        (clients: persist.ClientMap) => {
          assertNotUndefined(pullResponse);

          const clientToUpdate = clients.get(clientID);
          if (!clientToUpdate) {
            return persist.noClientUpdates;
          }

          if (isClientStateNotFoundResponse(pullResponse)) {
            const newClients = new Map(clients);
            newClients.delete(clientID);
            return {clients: newClients};
          }

          if (
            clientToUpdate.lastServerAckdMutationID >=
            (pullResponse as PullResponseOK).lastMutationID
          ) {
            return persist.noClientUpdates;
          }
          return {
            clients: new Map(clients).set(clientID, {
              ...clientToUpdate,
              lastServerAckdMutationID: (pullResponse as PullResponseOK)
                .lastMutationID,
            }),
          };
        },
        perdag,
      );
      return newClientMap;
    } catch (e) {
      logMutationRecoveryError(e, lc, stepDescription, delegate);
      return;
    } finally {
      await dagForOtherClientToClose?.close();
      lc.debug?.('End:', stepDescription);
    }
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
