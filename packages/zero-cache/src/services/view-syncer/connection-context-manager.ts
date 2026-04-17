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
 * Identifies one live websocket for a client slot.
 */
export type ConnectionSelector = {
  readonly clientID: string;
  readonly wsID: string;
};

type FetchConfig = ZeroConfig['query'];

export type HeaderOptions = {
  apiKey?: string | undefined;
  customHeaders?: Record<string, string> | undefined;
  allowedClientHeaders?: readonly string[] | undefined;
  cookie?: string | undefined;
  origin?: string | undefined;
};

export type ConnectionFetchContext = {
  url: string | undefined;
  allowedUrlPatterns: URLPattern[] | undefined;
  headerOptions: HeaderOptions;
};

/**
 * A snapshot of one live connection tracked by the manager.
 *
 * `revalidateAt` is only populated while the connection is `validated`.
 */
export type ConnectionContext = {
  state: ConnectionState;

  readonly clientID: string;
  readonly wsID: string;
  readonly userID: string | undefined;

  auth: Auth | undefined;

  readonly profileID: string | null;
  readonly baseCookie: string | null;
  readonly protocolVersion: number;

  revision: number;

  revalidateAt: number | undefined;

  readonly insertionOrder: number;

  readonly queryContext: ConnectionFetchContext;
  readonly pushContext: ConnectionFetchContext;
};

/**
 * Group-scoped auth state shared across the live connections.
 *
 * The background connection is the validated connection currently used for
 * shared background work. Retransform happens on a group level, and uses
 * the background connection's credential to refetch the latest queries.
 *
 * Since auth can be pinned to logged-out users, `userID` can be undefined
 * even after validation.
 */
export type GroupAuthState = {
  userID: string | undefined;
  validated: boolean;

  backgroundConnection: ConnectionSelector | undefined;
  retransformAt: number | undefined;
  // Defer all maintenance in case a transient failure occurs.
  maintenanceNotBeforeAt: number | undefined;
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
 * stored `userID` is confirmed as valid. The manager also tracks which
 * validated connection currently serves as the group's background connection.
 *
 * This is intentionally side-effect free.
 */
export class ConnectionContextManagerImpl implements ConnectionContextManager {
  readonly #lc: LogContext;

  // The live connection records, keyed by clientID
  readonly #connections = new Map<string, ConnectionContext>();
  readonly #group: GroupAuthState = {
    userID: undefined,
    backgroundConnection: undefined,
    retransformAt: undefined,
    maintenanceNotBeforeAt: undefined,
    validated: false,
  };

  readonly #validateLegacyJWT: ValidateLegacyJWT | undefined;

  readonly #now: () => number;
  readonly #revalidateIntervalMs: number | undefined;
  readonly #retransformIntervalMs: number | undefined;
  readonly #queryConfig: FetchConfig | undefined;
  readonly #pushConfig: FetchConfig | undefined;
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

    const sharedHeaders = {
      customHeaders: undefined,
      token: auth?.raw,
      origin: connectParams.origin,
      userID: connectParams.userID,
    };

    const connection: ConnectionContext = {
      state: 'provisional',

      clientID: connectParams.clientID,
      wsID: connectParams.wsID,
      revision: 0,
      userID: connectParams.userID,
      auth,

      profileID: connectParams.profileID,
      baseCookie: connectParams.baseCookie,
      protocolVersion: connectParams.protocolVersion,

      revalidateAt: undefined,

      queryContext: {
        url: this.#queryConfig?.url?.[0],
        allowedUrlPatterns: this.#queryConfig?.url?.map(compileUrlPattern),
        headerOptions: {
          ...sharedHeaders,
          apiKey: this.#queryConfig?.apiKey,
          allowedClientHeaders: this.#queryConfig?.allowedClientHeaders,
          cookie: this.#queryConfig?.forwardCookies
            ? connectParams.httpCookie
            : undefined,
        },
      },
      pushContext: {
        url: this.#pushConfig?.url?.[0],
        allowedUrlPatterns: this.#pushConfig?.url?.map(compileUrlPattern),
        headerOptions: {
          ...sharedHeaders,
          apiKey: this.#pushConfig?.apiKey,
          allowedClientHeaders: this.#pushConfig?.allowedClientHeaders,
          cookie: this.#pushConfig?.forwardCookies
            ? connectParams.httpCookie
            : undefined,
        },
      },

      insertionOrder: ++this.#nextInsertionOrder,
    };
    this.#connections.set(connection.clientID, connection);
    this.#refreshBackgroundConnectionContext();
    this.#updateBackgroundRetransformDeadline(false);
    return snapshotConnection(connection);
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

    if (body.userQueryURL) {
      connection.queryContext.url = body.userQueryURL;
    }
    if (body.userQueryHeaders) {
      connection.queryContext.headerOptions.customHeaders =
        body.userQueryHeaders;
    }
    if (body.userPushURL) {
      connection.pushContext.url = body.userPushURL;
    }
    if (body.userPushHeaders) {
      connection.pushContext.headerOptions.customHeaders = body.userPushHeaders;
    }

    connection.revision++;
    this.#demoteConnection(connection);

    return snapshotConnection(connection);
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
      connection.userID,
      body.auth,
      this.#validateLegacyJWT,
    );

    const authChanged = !authEquals(connection.auth, nextAuth);
    connection.auth = nextAuth;
    if (authChanged) {
      connection.revision++;
      this.#demoteConnection(connection);
    }

    return snapshotConnection(connection);
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

    if (this.#group.validated && this.#group.userID !== connection.userID) {
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

    if (!this.#group.validated) {
      this.#group.validated = true;
      this.#group.userID = connection.userID;
    }

    connection.state = 'validated';
    connection.revalidateAt = this.#nextRevalidateAt();
    this.#refreshBackgroundConnectionContext(connection);
    this.#updateBackgroundRetransformDeadline(false);

    return {
      connection: snapshotConnection(connection),
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
      selector !== undefined &&
      (backgroundConnection.clientID !== selector.clientID ||
        backgroundConnection.wsID !== selector.wsID ||
        backgroundConnection.revision !== revision)
    ) {
      return;
    }
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
    this.#group.maintenanceNotBeforeAt = Math.max(
      this.#group.maintenanceNotBeforeAt ?? 0,
      this.#now() + intervalMs,
    );
  }

  /** Returns the current live record for a client slot, if any. */
  getConnectionContext(
    selector: ConnectionSelector,
  ): Readonly<ConnectionContext> | undefined {
    return snapshotConnection(this.#getConnectionContext(selector));
  }

  /** Returns the live record for one websocket or throws if it is unavailable. */
  mustGetConnectionContext(
    selector: ConnectionSelector,
  ): Readonly<ConnectionContext> {
    return snapshotConnection(this.#mustGetConnectionContext(selector));
  }

  /** Returns the current background connection, if one exists. */
  getBackgroundConnectionContext(): Readonly<ConnectionContext> | undefined {
    return snapshotConnection(this.#getBackgroundConnectionContext());
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
    return snapshotGroup(this.#group);
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
        dueRevalidations.push(snapshotConnection(connection));
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

    const snapshot = snapshotConnection(connection);

    this.#connections.delete(connection.clientID);
    this.#refreshBackgroundConnectionContext();
    this.#updateBackgroundRetransformDeadline(false);

    return snapshot;
  }

  #demoteConnection(connection: ConnectionContext): void {
    connection.state = 'provisional';
    connection.revalidateAt = undefined;
    this.#refreshBackgroundConnectionContext();
    this.#updateBackgroundRetransformDeadline(false);
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
      this.#group.backgroundConnection = {
        clientID: preferred.clientID,
        wsID: preferred.wsID,
      };
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
    this.#group.backgroundConnection = nextBackgroundConnection
      ? {
          clientID: nextBackgroundConnection.clientID,
          wsID: nextBackgroundConnection.wsID,
        }
      : undefined;
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

  /**
   * Keeps the group background retransform deadline coherent with current
   * schedulability.
   *
   * When `reset` is false, this seeds a deadline only when shared retransform
   * is now possible and no deadline exists yet, preserving any existing
   * cadence. When `reset` is true, it starts a fresh interval from `#now()` if
   * retransform is schedulable, or clears the deadline if it is not.
   */
  #updateBackgroundRetransformDeadline(reset: boolean) {
    const backgroundConnection = this.#getBackgroundConnectionContext();
    if (!backgroundConnection || this.#retransformIntervalMs === undefined) {
      this.#group.retransformAt = undefined;
      return;
    }

    if (reset || this.#group.retransformAt === undefined) {
      this.#group.retransformAt = this.#now() + this.#retransformIntervalMs;
    }
  }

  #nextRevalidateAt() {
    return this.#revalidateIntervalMs === undefined
      ? undefined
      : this.#now() + this.#revalidateIntervalMs;
  }
}

function snapshotConnection<T extends ConnectionContext | undefined>(
  connection: T,
): T extends undefined ? T | undefined : Readonly<T> {
  if (!connection) {
    return undefined as T extends undefined ? T | undefined : Readonly<T>;
  }
  return {
    ...connection,
    queryContext: {
      ...connection.queryContext,
      headerOptions: {
        ...connection.queryContext.headerOptions,
        customHeaders: connection.queryContext.headerOptions.customHeaders
          ? {...connection.queryContext.headerOptions.customHeaders}
          : undefined,
      },
    },
    pushContext: {
      ...connection.pushContext,
      headerOptions: {
        ...connection.pushContext.headerOptions,
        customHeaders: connection.pushContext.headerOptions.customHeaders
          ? {...connection.pushContext.headerOptions.customHeaders}
          : undefined,
      },
    },
  } as T extends undefined ? T | undefined : Readonly<T>;
}

function snapshotGroup(group: GroupAuthState): Readonly<GroupAuthState> {
  return {
    ...group,
    backgroundConnection: group.backgroundConnection
      ? {...group.backgroundConnection}
      : undefined,
  };
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
