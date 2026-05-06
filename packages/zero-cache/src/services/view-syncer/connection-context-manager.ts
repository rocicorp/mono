import type {LogContext} from '@rocicorp/logger';
import type {InitConnectionBody} from '../../../../zero-protocol/src/connect.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../../zero-protocol/src/error-origin.ts';
import type {UpdateAuthBody} from '../../../../zero-protocol/src/update-auth.ts';
import {
  authEquals,
  resolveAuth,
  type Auth,
  type ValidateLegacyJWT,
} from '../../auth/auth.ts';
import type {ZeroConfig} from '../../config/zero-config.ts';
import {compileUrlPattern} from '../../custom/fetch.ts';
import {ProtocolErrorWithLevel} from '../../types/error-with-level.ts';
import type {ConnectParams} from '../../workers/connect-params.ts';

export type ConnectionState = 'provisional' | 'validated';

/**
 * Normalized user identity shared by live connection state and group auth state.
 * `id: null` means logged out.
 */
export type UserState = {readonly id: string | null};

/**
 * Delineates the two paths for validating a connection: either server can validate
 * the user's identity and return a definitive userID to trust, or we fall back to
 * trusting the one provided by the client in the incoming query params.
 */
export type ConnectionValidation =
  | {kind: 'client-fallback'}
  | {kind: 'server-validated'; validatedUserID: string | null};

/**
 * Identifies one live websocket for a client slot.
 */
export type ConnectionSelector = {
  readonly clientID: string;
  readonly wsID: string;
};

type FetchConfig = ZeroConfig['query'];

export type HeaderOptions = {
  readonly apiKey?: string | undefined;
  readonly customHeaders?: Readonly<Record<string, string>> | undefined;
  readonly allowedClientHeaders?: readonly string[] | undefined;
  readonly cookie?: string | undefined;
  readonly origin?: string | undefined;
};

export type ConnectionFetchContext = {
  readonly url: string | undefined;
  readonly allowedUrlPatterns: readonly URLPattern[] | undefined;
  readonly headerOptions: HeaderOptions;
};

/**
 * A snapshot of one live connection tracked by the manager.
 *
 * `revalidateAt` is only populated while the connection is `validated`.
 */
export type ConnectionContext = {
  readonly state: ConnectionState;

  readonly clientID: string;
  readonly wsID: string;
  readonly user: UserState;

  readonly auth: Auth | undefined;

  readonly profileID: string | null;
  readonly baseCookie: string | null;
  readonly protocolVersion: number;

  readonly revision: number;

  readonly revalidateAt: number | undefined;

  readonly insertionOrder: number;

  readonly queryContext: ConnectionFetchContext;
  readonly mutateContext: ConnectionFetchContext;
};

/**
 * Group-scoped auth state shared across the live connections.
 *
 * The background connection is the validated connection currently used for
 * shared background work. Retransform happens on a group level, and uses
 * the background connection's credential to refetch the latest queries.
 */
export type GroupAuthState = {
  readonly pinnedUser: UserState | undefined;

  readonly backgroundConnection: ConnectionSelector | undefined;
  readonly retransformAt: number | undefined;
  // Defer all maintenance in case a transient failure occurs.
  readonly maintenanceNotBeforeAt: number | undefined;
};

export type ConnectionContextManager = {
  registerConnection(
    selector: ConnectionSelector,
    connectParams: ConnectParams,
    auth?: Auth,
  ): Readonly<ConnectionContext>;

  initConnection(
    selector: ConnectionSelector,
    body: InitConnectionBody,
  ): Readonly<ConnectionContext>;

  updateAuth(
    selector: ConnectionSelector,
    body: UpdateAuthBody,
  ): Promise<Readonly<ConnectionContext>>;

  validateConnection(
    selector: ConnectionSelector,
    revision: number,
    validation: ConnectionValidation,
  ):
    | Readonly<{
        connection: ConnectionContext;
        group: GroupAuthState;
      }>
    | undefined;

  failConnection(
    selector: ConnectionSelector,
    revision: number,
  ): Readonly<ConnectionContext> | undefined;
  closeConnection(
    selector: ConnectionSelector,
  ): Readonly<ConnectionContext> | undefined;

  markBackgroundRetransformSuccess(
    selector: ConnectionSelector,
    revision: number,
  ): void;

  setSharedRetransformReady(ready: boolean): void;

  deferMaintenance(kind: 'revalidate' | 'retransform'): void;

  getConnectionContext(
    selector: ConnectionSelector,
  ): Readonly<ConnectionContext> | undefined;
  mustGetConnectionContext(
    selector: ConnectionSelector,
  ): Readonly<ConnectionContext>;

  getBackgroundConnectionContext(): Readonly<ConnectionContext> | undefined;
  mustGetBackgroundConnectionContext(): Readonly<ConnectionContext>;

  getGroupState(): Readonly<GroupAuthState>;

  planMaintenance(): {
    dueRevalidations: Readonly<ConnectionContext>[];
    dueRetransform: boolean;
    earliestDeadlineAt: number | undefined;
  };
};

/**
 * State machine for the auth state of a single `ViewSyncerService`.
 *
 * Connections are registered as `provisional`, optionally backfilled with
 * `initConnection` metadata, and then promoted to `validated` once their
 * effective `userID` is confirmed as valid. The manager also tracks which
 * validated connection currently serves as the group's background connection.
 */
export class ConnectionContextManagerImpl implements ConnectionContextManager {
  readonly #lc: LogContext;

  // The live connection records, keyed by clientID
  readonly #connections = new Map<string, ConnectionContext>();
  #group: GroupAuthState = {
    pinnedUser: undefined,
    backgroundConnection: undefined,
    retransformAt: undefined,
    maintenanceNotBeforeAt: undefined,
  };

  readonly #validateLegacyJWT: ValidateLegacyJWT | undefined;

  readonly #now: () => number;
  readonly #revalidateIntervalMs: number | undefined;
  readonly #retransformIntervalMs: number | undefined;
  readonly #queryConfig: FetchConfig | undefined;
  readonly #pushConfig: FetchConfig | undefined;
  #sharedRetransformReady = false;
  #nextInsertionOrder = 0;

  constructor(
    lc: LogContext,
    revalidateIntervalSeconds?: number,
    retransformIntervalSeconds?: number,
    queryConfig?: FetchConfig,
    pushConfig?: FetchConfig,
    validateLegacyJWT?: ValidateLegacyJWT,
    now?: () => number,
  ) {
    this.#lc = lc;
    this.#now = now ?? Date.now;
    this.#revalidateIntervalMs =
      revalidateIntervalSeconds === undefined
        ? undefined
        : revalidateIntervalSeconds * 1000;
    this.#retransformIntervalMs =
      retransformIntervalSeconds === undefined
        ? undefined
        : retransformIntervalSeconds * 1000;
    this.#queryConfig = queryConfig;
    this.#pushConfig = pushConfig;
    this.#validateLegacyJWT = validateLegacyJWT;
  }

  /**
   * Creates or replaces the live record for a websocket connection.
   *
   * Re-registering the same `clientID` drops the old socket record and starts
   * the replacement back in `provisional` state.
   */
  registerConnection(
    selector: ConnectionSelector,
    connectParams: ConnectParams,
    auth?: Auth,
  ): Readonly<ConnectionContext> {
    this.#removeConnection(selector);

    const getContext = (type: 'query' | 'mutate'): ConnectionFetchContext => {
      const config = type === 'query' ? this.#queryConfig : this.#pushConfig;

      return {
        url: config?.url?.[0],
        allowedUrlPatterns: config?.url?.map(compileUrlPattern),
        headerOptions: {
          customHeaders: undefined,
          origin: connectParams.origin,
          apiKey: config?.apiKey,
          allowedClientHeaders: cloneAllowedClientHeaders(
            config?.allowedClientHeaders,
          ),
          cookie: config?.forwardCookies ? connectParams.httpCookie : undefined,
        },
      };
    };

    const connection: ConnectionContext = {
      state: 'provisional',

      clientID: connectParams.clientID,
      wsID: connectParams.wsID,
      revision: 0,
      user: {id: connectParams.userID ?? null},
      auth,

      profileID: connectParams.profileID,
      baseCookie: connectParams.baseCookie,
      protocolVersion: connectParams.protocolVersion,

      revalidateAt: undefined,

      queryContext: getContext('query'),
      mutateContext: getContext('mutate'),

      insertionOrder: ++this.#nextInsertionOrder,
    };
    this.#storeConnection(connection);
    this.#refreshBackgroundConnectionContext();
    this.#updateBackgroundRetransformDeadline(false);
    return connection;
  }

  /**
   * Backfills `initConnection` data for sockets that were registered before the
   * client could send its full init payload.
   *
   * This updates metadata only; it does not validate the connection.
   */
  initConnection(
    selector: ConnectionSelector,
    body: InitConnectionBody,
  ): Readonly<ConnectionContext> {
    const connection = this.#mustGetConnectionContext(selector);

    let queryContext = connection.queryContext;
    let mutateContext = connection.mutateContext;

    if (body.userQueryURL) {
      queryContext = {
        ...queryContext,
        url: body.userQueryURL,
      };
    }
    if (body.userQueryHeaders) {
      queryContext = {
        ...queryContext,
        headerOptions: {
          ...queryContext.headerOptions,
          customHeaders: cloneCustomHeaders(body.userQueryHeaders),
        },
      };
    }
    if (body.userPushURL) {
      mutateContext = {
        ...mutateContext,
        url: body.userPushURL,
      };
    }
    if (body.userPushHeaders) {
      mutateContext = {
        ...mutateContext,
        headerOptions: {
          ...mutateContext.headerOptions,
          customHeaders: cloneCustomHeaders(body.userPushHeaders),
        },
      };
    }

    return this.#demoteConnection({
      ...connection,
      revision: connection.revision + 1,
      queryContext,
      mutateContext,
    });
  }

  /**
   * A material auth change demotes the connection back to provisional until it
   * is validated again.
   */
  async updateAuth(
    selector: ConnectionSelector,
    body: UpdateAuthBody,
  ): Promise<Readonly<ConnectionContext>> {
    const connection = this.#mustGetConnectionContext(selector);

    const nextAuth = await resolveAuth(
      this.#lc,
      connection.auth,
      connection.user.id,
      body.auth,
      this.#validateLegacyJWT,
    );

    const authChanged = !authEquals(connection.auth, nextAuth);
    if (authChanged) {
      return this.#demoteConnection({
        ...connection,
        auth: nextAuth,
        revision: connection.revision + 1,
      });
    }

    if (nextAuth === connection.auth) {
      return connection;
    }

    return this.#storeConnection({
      ...connection,
      auth: nextAuth,
    });
  }

  /**
   * Validates one connection against the group's pinned `userID`.
   *
   * The first successful validation binds the group `userID`. Later
   * validations must match it. Validation also refreshes the connection's
   * revalidation deadline and may pick the connection as the group
   * background connection if none is currently available. If the websocket is
   * gone by the time async validation finishes, this becomes a no-op.
   */
  validateConnection(
    selector: ConnectionSelector,
    revision: number,
    validation: ConnectionValidation,
  ):
    | Readonly<{
        connection: ConnectionContext;
        group: GroupAuthState;
      }>
    | undefined {
    const connection = this.#getConnectionContext(selector);
    if (!connection) {
      return undefined;
    }

    if (connection.revision !== revision) {
      this.#lc.debug?.('Skipping validateConnection for stale revision', {
        clientID: selector.clientID,
        attemptedRevision: revision,
        currentRevision: connection.revision,
      });
      return undefined;
    }

    let validatedUserState: UserState | undefined;

    // If the API server has validated the user's identity, we ensure that
    // the connection's claimed userID matches it.
    if (validation.kind === 'server-validated') {
      validatedUserState = {id: validation.validatedUserID};

      // Check that the ws connection userID provided by the client
      // matches the validated userID from the API server.
      if (connection.user.id !== validatedUserState.id) {
        throw new ProtocolErrorWithLevel(
          {
            kind: ErrorKind.Unauthorized,
            message:
              'Connection userID does not match validated server userID.',
            origin: ErrorOrigin.ZeroCache,
          },
          'warn',
        );
      }
    }

    // The incoming user state is either the validated user state from the server
    // or the WS client's claimed user state if no server validation occurred.
    const incomingUserState = validatedUserState ?? connection.user;

    // Once a client group is validated, every later validated connection must
    // agree with that pinned identity.
    if (
      this.#group.pinnedUser !== undefined &&
      this.#group.pinnedUser.id !== incomingUserState.id
    ) {
      throw new ProtocolErrorWithLevel(
        {
          kind: ErrorKind.Unauthorized,
          message:
            'Client groups are pinned to a single userID. Connection userID does not match existing client group userID.',
          origin: ErrorOrigin.ZeroCache,
        },
        'warn',
      );
    }

    if (this.#group.pinnedUser === undefined) {
      this.#setGroup({
        ...this.#group,
        pinnedUser: incomingUserState,
      });
    }

    const validatedConnection = this.#storeConnection({
      ...connection,
      state: 'validated',
      revalidateAt: this.#nextRevalidateAt(),
    });
    this.#refreshBackgroundConnectionContext(validatedConnection);
    this.#updateBackgroundRetransformDeadline(false);

    return {
      connection: validatedConnection,
      group: this.getGroupState(),
    };
  }

  /** Removes one connection due to failed auth and updates all derived background/deadline state. */
  failConnection(
    selector: ConnectionSelector,
    revision: number,
  ): ConnectionContext | undefined {
    return this.#removeConnection(selector, revision);
  }

  /** Removes one disconnected connection and updates all derived background/deadline state. */
  closeConnection(selector: ConnectionSelector): ConnectionContext | undefined {
    return this.#removeConnection(selector);
  }

  /**
   * Records a successful background retransform. This starts a fresh interval
   * from the manager clock when shared retransform is schedulable, or
   * clears the deadline if it is not.
   */
  markBackgroundRetransformSuccess(
    selector: ConnectionSelector,
    revision: number,
  ): void {
    const backgroundConnection = this.#getBackgroundConnectionContext();
    if (!backgroundConnection) {
      return;
    }
    if (
      backgroundConnection.clientID !== selector.clientID ||
      backgroundConnection.wsID !== selector.wsID ||
      backgroundConnection.revision !== revision
    ) {
      return;
    }
    this.#updateBackgroundRetransformDeadline(true);
  }

  setSharedRetransformReady(ready: boolean): void {
    if (this.#sharedRetransformReady === ready) {
      return;
    }
    this.#sharedRetransformReady = ready;
    this.#updateBackgroundRetransformDeadline(true);
  }

  deferMaintenance(kind: 'revalidate' | 'retransform'): void {
    const intervalMs =
      kind === 'revalidate'
        ? this.#revalidateIntervalMs
        : this.#retransformIntervalMs;
    if (intervalMs === undefined) {
      return;
    }
    this.#setGroup({
      ...this.#group,
      maintenanceNotBeforeAt: Math.max(
        this.#group.maintenanceNotBeforeAt ?? 0,
        this.#now() + intervalMs,
      ),
    });
  }

  /** Returns the current live record for a client slot, if any. */
  getConnectionContext(
    selector: ConnectionSelector,
  ): Readonly<ConnectionContext> | undefined {
    return this.#getConnectionContext(selector);
  }

  /** Returns the live record for one websocket or throws if it is unavailable. */
  mustGetConnectionContext(
    selector: ConnectionSelector,
  ): Readonly<ConnectionContext> {
    return this.#mustGetConnectionContext(selector);
  }

  /** Returns the current background connection, if one exists. */
  getBackgroundConnectionContext(): Readonly<ConnectionContext> | undefined {
    return this.#getBackgroundConnectionContext();
  }

  mustGetBackgroundConnectionContext(): Readonly<ConnectionContext> {
    const backgroundConnection = this.#getBackgroundConnectionContext();
    if (!backgroundConnection) {
      throw new ProtocolErrorWithLevel(
        {
          kind: ErrorKind.InvalidConnectionRequest,
          message:
            'No validated connection is available for shared query work.',
          origin: ErrorOrigin.ZeroCache,
        },
        'warn',
      );
    }
    return backgroundConnection;
  }

  /** Returns the shared group auth state. */
  getGroupState(): Readonly<GroupAuthState> {
    return this.#group;
  }

  /**
   * Reports which maintenance work is currently due.
   *
   * The result is a pure snapshot: callers decide which actions to run and
   * when to wake up next. `earliestDeadlineAt` is the earliest outstanding
   * maintenance deadline, including overdue work, unless a transient failure
   * has deferred all scheduled maintenance until `maintenanceNotBeforeAt`.
   */
  planMaintenance(): {
    dueRevalidations: Readonly<ConnectionContext>[];
    dueRetransform: boolean;
    earliestDeadlineAt: number | undefined;
  } {
    const dueRevalidations: Readonly<ConnectionContext>[] = [];
    const now = this.#now();
    let earliestDeadlineAt = this.#group.retransformAt;

    for (const connection of this.#connections.values()) {
      if (
        connection.state !== 'validated' ||
        connection.revalidateAt === undefined
      ) {
        continue;
      }
      if (connection.revalidateAt <= now) {
        dueRevalidations.push(connection);
      }
      earliestDeadlineAt = minDefined(
        earliestDeadlineAt,
        connection.revalidateAt,
      );
    }

    const dueRetransform =
      this.#group.retransformAt !== undefined &&
      this.#group.retransformAt <= now;
    const maintenanceNotBeforeAt = this.#group.maintenanceNotBeforeAt;

    if (
      maintenanceNotBeforeAt !== undefined &&
      maintenanceNotBeforeAt > now &&
      earliestDeadlineAt !== undefined
    ) {
      return {
        dueRevalidations: [],
        dueRetransform: false,
        earliestDeadlineAt: Math.max(
          earliestDeadlineAt,
          maintenanceNotBeforeAt,
        ),
      };
    }

    return {
      dueRevalidations: dueRevalidations.sort(compareByInsertionOrder),
      dueRetransform,
      earliestDeadlineAt,
    };
  }

  #removeConnection(
    selector: ConnectionSelector,
    revision?: number,
  ): Readonly<ConnectionContext> | undefined {
    const connection = this.#getConnectionContext(selector);

    if (!connection) {
      return undefined;
    }

    // If the revision has changed, we should not remove the connection
    if (revision !== undefined && connection.revision !== revision) {
      this.#lc.debug?.('Ignoring failConnection for stale revision', {
        clientID: selector.clientID,
        wsID: selector.wsID,
        attemptedRevision: revision,
        currentRevision: connection.revision,
      });
      return undefined;
    }

    this.#connections.delete(connection.clientID);
    this.#refreshBackgroundConnectionContext();
    this.#updateBackgroundRetransformDeadline(false);

    return connection;
  }

  #demoteConnection(connection: ConnectionContext): ConnectionContext {
    const demotedConnection = this.#storeConnection({
      ...connection,
      state: 'provisional',
      revalidateAt: undefined,
    });
    this.#refreshBackgroundConnectionContext();
    this.#updateBackgroundRetransformDeadline(false);
    return demotedConnection;
  }

  /**
   * Keeps the background connection sticky while it remains validated.
   *
   * When a newly validated `preferred` connection is provided, it is promoted
   * only if there is no current validated background connection. Otherwise the
   * existing background connection stays in place until it disappears or is
   * demoted, at which point the newest validated connection is selected.
   */
  #refreshBackgroundConnectionContext(preferred?: ConnectionContext): void {
    if (preferred?.state === 'validated') {
      const currentBackgroundConnection =
        this.#getBackgroundConnectionContext();
      if (
        currentBackgroundConnection?.clientID === preferred.clientID &&
        currentBackgroundConnection.wsID === preferred.wsID
      ) {
        return;
      }
      if (currentBackgroundConnection !== undefined) {
        return;
      }
      this.#setBackgroundConnection({
        clientID: preferred.clientID,
        wsID: preferred.wsID,
      });
      this.#lc.debug?.('Selected background connection for shared auth work', {
        clientID: preferred.clientID,
        wsID: preferred.wsID,
        revision: preferred.revision,
        reason: 'preferred-validated',
      });
      return;
    }

    const currentBackgroundConnection = this.#getBackgroundConnectionContext();
    if (currentBackgroundConnection?.state === 'validated') {
      return;
    }

    const nextBackgroundConnection = [...this.#connections.values()]
      .filter(connection => connection.state === 'validated')
      .sort(comparePreferredValidatedConnection)
      .at(0);
    this.#setBackgroundConnection(
      nextBackgroundConnection
        ? {
            clientID: nextBackgroundConnection.clientID,
            wsID: nextBackgroundConnection.wsID,
          }
        : undefined,
    );
    if (nextBackgroundConnection) {
      this.#lc.debug?.('Selected background connection for shared auth work', {
        clientID: nextBackgroundConnection.clientID,
        wsID: nextBackgroundConnection.wsID,
        revision: nextBackgroundConnection.revision,
        reason: 'fallback-validated',
      });
    }
  }

  #getBackgroundConnectionContext(): ConnectionContext | undefined {
    const backgroundConnection = this.#group.backgroundConnection;
    if (!backgroundConnection) {
      return undefined;
    }
    return this.#getConnectionContext(backgroundConnection);
  }

  #getConnectionContext(
    selector: ConnectionSelector,
  ): ConnectionContext | undefined {
    const connection = this.#connections.get(selector.clientID);
    if (!connection) {
      return undefined;
    }
    if (connection.wsID !== selector.wsID) {
      return undefined;
    }
    return connection;
  }

  #mustGetConnectionContext(selector: ConnectionSelector): ConnectionContext {
    const connection = this.#getConnectionContext(selector);

    if (!connection) {
      throw new ProtocolErrorWithLevel(
        {
          kind: ErrorKind.InvalidConnectionRequest,
          message:
            'Connection auth state was not available for this websocket.',
          origin: ErrorOrigin.ZeroCache,
        },
        'warn',
      );
    }

    return connection;
  }

  #storeConnection(connection: ConnectionContext): ConnectionContext {
    this.#connections.set(connection.clientID, connection);
    return connection;
  }

  #setGroup(group: GroupAuthState): GroupAuthState {
    this.#group = group;
    return group;
  }

  #setBackgroundConnection(
    backgroundConnection: ConnectionSelector | undefined,
  ) {
    if (
      sameConnectionSelector(
        this.#group.backgroundConnection,
        backgroundConnection,
      )
    ) {
      return;
    }
    this.#setGroup({
      ...this.#group,
      backgroundConnection: backgroundConnection
        ? {...backgroundConnection}
        : undefined,
    });
  }

  /**
   * Keeps the group background retransform deadline coherent with current
   * schedulability.
   *
   * When `reset` is false, this seeds a deadline only when shared retransform
   * is now possible and no deadline exists yet, preserving any existing
   * cadence. When `reset` is true, it starts a fresh interval from `#now()` if
   * retransform is schedulable for the current ready ViewSyncer instance, or
   * clears the deadline if it is not.
   */
  #updateBackgroundRetransformDeadline(reset: boolean) {
    const backgroundConnection = this.#getBackgroundConnectionContext();
    if (
      !backgroundConnection ||
      this.#retransformIntervalMs === undefined ||
      !this.#sharedRetransformReady
    ) {
      if (this.#group.retransformAt !== undefined) {
        this.#setGroup({
          ...this.#group,
          retransformAt: undefined,
        });
      }
      return;
    }

    if (reset || this.#group.retransformAt === undefined) {
      this.#setGroup({
        ...this.#group,
        retransformAt: this.#now() + this.#retransformIntervalMs,
      });
    }
  }

  #nextRevalidateAt() {
    return this.#revalidateIntervalMs === undefined
      ? undefined
      : this.#now() + this.#revalidateIntervalMs;
  }
}

function compareByInsertionOrder(
  a: Pick<ConnectionContext, 'insertionOrder' | 'wsID'>,
  b: Pick<ConnectionContext, 'insertionOrder' | 'wsID'>,
) {
  return a.insertionOrder - b.insertionOrder || a.wsID.localeCompare(b.wsID);
}

function comparePreferredValidatedConnection(
  a: Pick<ConnectionContext, 'insertionOrder' | 'wsID'>,
  b: Pick<ConnectionContext, 'insertionOrder' | 'wsID'>,
) {
  return b.insertionOrder - a.insertionOrder || b.wsID.localeCompare(a.wsID);
}

function minDefined(a: number | undefined, b: number | undefined) {
  if (a === undefined) {
    return b;
  }
  if (b === undefined) {
    return a;
  }
  return Math.min(a, b);
}

function sameConnectionSelector(
  a: ConnectionSelector | undefined,
  b: ConnectionSelector | undefined,
) {
  return a?.clientID === b?.clientID && a?.wsID === b?.wsID;
}

function cloneCustomHeaders(
  headers: Readonly<Record<string, string>> | undefined,
) {
  return headers ? {...headers} : undefined;
}

function cloneAllowedClientHeaders(headers: readonly string[] | undefined) {
  return headers ? [...headers] : undefined;
}
