import type {Auth} from '../../auth/auth.ts';
import {ProtocolErrorWithLevel} from '../../types/error-with-level.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../../zero-protocol/src/error-origin.ts';

export type PrincipalID = string | null;
export type PrincipalSource = 'query' | 'userID';
export type ConnectionState = 'provisional' | 'validated';

/** Identifies one live websocket for a client slot. */
export type ConnectionSelector = {
  clientID: string;
  wsID: string;
};

/**
 * A snapshot of one live connection tracked by the coordinator.
 *
 * `principalID`, `principalSource`, and `revalidateAt` are only populated
 * while the connection is `validated`.
 */
export type ConnectionAuthState = {
  clientID: string;
  wsID: string;
  userID: string;
  auth: Auth | undefined;
  httpCookie: string | undefined;
  origin: string | undefined;
  userQueryURL: string | undefined;
  userQueryHeaders: Record<string, string> | undefined;
  state: ConnectionState;
  principalID: PrincipalID | undefined;
  principalSource: PrincipalSource | undefined;
  revalidateAt: number | undefined;
  insertionOrder: number;
};

/**
 * Group-scoped auth state shared across the live connections.
 *
 * `selectedConnection` points at the validated connection currently used for
 * shared background work. `nextRetransformAt` is a group-level deadline rather
 * than a per-connection one.
 */
export type GroupAuthState = {
  principalID: PrincipalID | undefined;
  principalSource: PrincipalSource | undefined;
  selectedConnection: ConnectionSelector | undefined;
  nextRetransformAt: number | undefined;
};

/**
 * Validation is the shared transition that binds the client group principal,
 * upgrades a connection to validated, and refreshes its maintenance deadline.
 *
 * The first successful validation binds the group's `principalID`. Later
 * validations must use the same principal or they throw `Unauthorized`.
 */
export type ConnectionValidationResult = {
  connection: ConnectionAuthState;
  group: GroupAuthState;
};

export type ConnectionRemovalResult = {
  status: 'failed' | 'closed';
  connection: ConnectionAuthState;
};

/**
 * State machine for the auth state of a single `ViewSyncerService`.
 *
 * Connections are registered as `provisional`, optionally backfilled with
 * `initConnection` metadata, and then promoted to `validated` once their
 * principal is confirmed. The coordinator also tracks which validated
 * connection is currently selected for shared background work.
 *
 * This is intentionally side-effect free: it does not fetch `/query`, touch
 * storage, or close websockets. Callers drive all transitions and poll
 * `planMaintenance()` to decide what work is due.
 */
export class ConnectionAuthCoordinator {
  readonly #connections = new Map<string, ConnectionAuthState>();
  readonly #group: GroupAuthState = {
    principalID: undefined,
    principalSource: undefined,
    selectedConnection: undefined,
    nextRetransformAt: undefined,
  };
  // Helper for testing to control the coordinator clock.
  readonly #now: () => number;
  readonly #revalidateIntervalMs: number | undefined;
  readonly #retransformIntervalMs: number | undefined;
  #nextInsertionOrder = 0;

  constructor(
    revalidateIntervalSeconds?: number,
    retransformIntervalSeconds?: number,
    now?: () => number,
  ) {
    this.#now = now ?? Date.now;
    this.#revalidateIntervalMs = intervalMs(revalidateIntervalSeconds);
    this.#retransformIntervalMs = intervalMs(retransformIntervalSeconds);
  }

  /**
   * Creates or replaces the live record for a websocket connection.
   *
   * Re-registering the same `clientID` drops the old socket record and starts
   * the replacement back in `provisional` state.
   */
  registerConnection(
    clientID: string,
    wsID: string,
    userID: string,
    auth: Auth | undefined,
    httpCookie: string | undefined,
    origin: string | undefined,
    userQueryURL: string | undefined,
    userQueryHeaders: Record<string, string> | undefined,
  ): Readonly<ConnectionAuthState> {
    this.#removeConnection(clientID);

    const connection: ConnectionAuthState = {
      clientID: clientID,
      wsID: wsID,
      userID: userID,
      auth: auth,
      httpCookie: httpCookie,
      origin: origin,
      userQueryURL: userQueryURL,
      userQueryHeaders: userQueryHeaders,
      state: 'provisional',
      principalID: undefined,
      principalSource: undefined,
      revalidateAt: undefined,
      insertionOrder: ++this.#nextInsertionOrder,
    };
    this.#connections.set(connection.clientID, connection);
    this.#refreshSelectedConnection();
    this.#syncBackgroundRetransformDeadline();
    return snapshotLiveConnection(connection);
  }

  /**
   * Backfills `initConnection` data for sockets that were registered before the
   * client could send its full init payload.
   *
   * This updates metadata only; it does not validate the connection.
   */
  applyInitConnection(
    clientID: string,
    wsID: string,
    httpCookie: string | undefined,
    origin: string | undefined,
    userQueryURL: string | undefined,
    userQueryHeaders: Record<string, string> | undefined,
  ): Readonly<ConnectionAuthState> {
    const connection = this.#requireMutableConnection(clientID, wsID);

    connection.httpCookie = httpCookie;
    connection.origin = origin;
    connection.userQueryURL = userQueryURL;
    connection.userQueryHeaders = userQueryHeaders;

    return snapshotLiveConnection(connection);
  }

  /**
   * A material auth change demotes the connection back to provisional until it
   * is validated again.
   *
   * Auth equality is based on the `Auth` type and raw token payload.
   */
  updateConnectionAuth(
    clientID: string,
    wsID: string,
    auth: Auth | undefined,
  ): Readonly<ConnectionAuthState> {
    const connection = this.#requireMutableConnection(clientID, wsID);

    const authChanged = !authEquals(connection.auth, auth);
    connection.auth = auth;
    if (authChanged) {
      this.#demoteConnection(connection);
    }

    return snapshotLiveConnection(connection);
  }

  /**
   * Validates one connection against the group principal.
   *
   * The first successful validation binds the group principal. Later
   * validations must match it. Validation also refreshes the connection's
   * revalidation deadline and may select the connection for shared background
   * work if no validated connection is currently selected. If the websocket is
   * gone by the time async validation finishes, this becomes a no-op.
   */
  validateConnection(
    clientID: string,
    wsID: string,
    principalID: PrincipalID,
    principalSource: PrincipalSource,
    revalidateAt?: number,
  ): Readonly<ConnectionValidationResult> | undefined {
    const connection = this.#getMutableConnection(clientID, wsID);
    if (!connection) {
      return undefined;
    }

    const expectedPrincipalID = this.#group.principalID;
    if (
      expectedPrincipalID !== undefined &&
      expectedPrincipalID !== principalID
    ) {
      throwPrincipalMismatch();
    }

    if (expectedPrincipalID === undefined) {
      this.#group.principalID = principalID;
      this.#group.principalSource = principalSource;
    } else if (
      this.#group.principalSource === 'userID' &&
      principalSource === 'query'
    ) {
      this.#group.principalSource = 'query';
    }

    connection.state = 'validated';
    connection.principalID = principalID;
    connection.principalSource = principalSource;
    connection.revalidateAt = revalidateAt ?? this.#nextRevalidateAt();
    this.#refreshSelectedConnection(connection);
    this.#syncBackgroundRetransformDeadline();

    return {
      connection: snapshotLiveConnection(connection),
      group: this.getGroupState(),
    };
  }

  /** Removes one failing connection and updates all derived selection/deadline state. */
  failConnection(
    selector: ConnectionSelector,
  ): ConnectionRemovalResult | undefined {
    return this.#removeWithStatus(selector, 'failed');
  }

  /** Removes one closed connection and updates all derived selection/deadline state. */
  closeConnection(
    selector: ConnectionSelector,
  ): ConnectionRemovalResult | undefined {
    return this.#removeWithStatus(selector, 'closed');
  }

  /**
   * Records a successful background retransform. This starts a fresh interval
   * from the coordinator clock when shared retransform is schedulable, or
   * clears the deadline if it is not.
   */
  markBackgroundRetransformSuccess(): void {
    this.#updateBackgroundRetransformDeadline(true);
  }

  /** Returns the current live record for a client slot, if any. */
  getConnection(clientID: string): Readonly<ConnectionAuthState> | undefined {
    return snapshotConnection(this.#connections.get(clientID));
  }

  /** Returns the live record for one websocket or throws if it is unavailable. */
  requireConnection(
    clientID: string,
    wsID: string,
  ): Readonly<ConnectionAuthState> {
    return snapshotLiveConnection(
      this.#requireMutableConnection(clientID, wsID),
    );
  }

  /** Returns the currently selected validated connection, if one exists. */
  getSelectedConnection(): Readonly<ConnectionAuthState> | undefined {
    const selected = this.#group.selectedConnection;
    if (!selected) {
      return undefined;
    }
    const connection = this.#connections.get(selected.clientID);
    if (!connection || connection.wsID !== selected.wsID) {
      return undefined;
    }
    return snapshotConnection(connection);
  }

  /** Returns the shared group auth state. */
  getGroupState(): Readonly<GroupAuthState> {
    return snapshotGroup(this.#group);
  }

  /** Lists all live connections in registration order. */
  listConnections(): Readonly<ConnectionAuthState>[] {
    return [...this.#connections.values()]
      .sort(compareByInsertionOrder)
      .map(snapshotLiveConnection);
  }

  /**
   * Reports which maintenance work is currently due.
   *
   * The result is a pure snapshot: callers decide when to wake up and which
   * actions to run.
   */
  planMaintenance(): {
    dueRevalidations: Readonly<ConnectionAuthState>[];
    dueRetransform: boolean;
    nextWakeAt: number | undefined;
  } {
    const dueRevalidations: Readonly<ConnectionAuthState>[] = [];
    const now = this.#now();
    let nextWakeAt = this.#group.nextRetransformAt;

    for (const connection of this.#connections.values()) {
      if (
        connection.state !== 'validated' ||
        connection.revalidateAt === undefined
      ) {
        continue;
      }
      if (connection.revalidateAt <= now) {
        dueRevalidations.push(snapshotLiveConnection(connection));
      }
      nextWakeAt = minDefined(nextWakeAt, connection.revalidateAt);
    }

    return {
      dueRevalidations: dueRevalidations.sort(compareByInsertionOrder),
      dueRetransform:
        this.#group.nextRetransformAt !== undefined &&
        this.#group.nextRetransformAt <= now,
      nextWakeAt,
    };
  }

  #removeWithStatus(
    selector: ConnectionSelector,
    status: 'failed' | 'closed',
  ): Readonly<ConnectionRemovalResult> | undefined {
    const connection = this.#getMutableConnection(
      selector.clientID,
      selector.wsID,
    );
    if (!connection) {
      return undefined;
    }

    const snapshot = snapshotLiveConnection(connection);
    this.#removeConnection(selector.clientID);
    return {status, connection: snapshot};
  }

  #removeConnection(clientID: string): void {
    this.#connections.delete(clientID);
    this.#refreshSelectedConnection();
    this.#syncBackgroundRetransformDeadline();
  }

  #demoteConnection(connection: ConnectionAuthState): void {
    connection.state = 'provisional';
    connection.principalID = undefined;
    connection.principalSource = undefined;
    connection.revalidateAt = undefined;
    this.#refreshSelectedConnection();
    this.#syncBackgroundRetransformDeadline();
  }

  /**
   * Keeps the group background retransform deadline coherent with current
   * schedulability. This seeds a deadline only when shared retransform is now
   * possible and no deadline exists yet; it preserves an existing cadence
   * otherwise.
   */
  #syncBackgroundRetransformDeadline(): void {
    this.#updateBackgroundRetransformDeadline(false);
  }

  #refreshSelectedConnection(preferred?: ConnectionAuthState): void {
    if (preferred?.state === 'validated') {
      const currentSelected = this.getSelectedConnection();
      if (
        currentSelected?.clientID === preferred.clientID &&
        currentSelected.wsID === preferred.wsID
      ) {
        return;
      }
      if (currentSelected !== undefined) {
        return;
      }
      this.#group.selectedConnection = selectorOf(preferred);
      return;
    }

    const currentSelected = this.getSelectedConnection();
    if (currentSelected?.state === 'validated') {
      return;
    }

    const nextSelected = [...this.#connections.values()]
      .filter(connection => connection.state === 'validated')
      .sort(comparePreferredValidatedConnection)
      .at(0);
    this.#group.selectedConnection = nextSelected
      ? selectorOf(nextSelected)
      : undefined;
  }

  #getMutableConnection(
    clientID: string,
    wsID: string,
  ): ConnectionAuthState | undefined {
    const connection = this.#connections.get(clientID);
    if (!connection) {
      return undefined;
    }
    if (connection.wsID !== wsID) {
      return undefined;
    }
    return connection;
  }

  #requireMutableConnection(
    clientID: string,
    wsID: string,
  ): ConnectionAuthState {
    return (
      this.#getMutableConnection(clientID, wsID) ?? invalidConnectionRequest()
    );
  }

  #updateBackgroundRetransformDeadline(reset: boolean) {
    const selected = this.getSelectedConnection();
    if (!selected || this.#retransformIntervalMs === undefined) {
      this.#group.nextRetransformAt = undefined;
      return;
    }

    if (reset || this.#group.nextRetransformAt === undefined) {
      this.#group.nextRetransformAt = this.#now() + this.#retransformIntervalMs;
    }
  }

  #nextRevalidateAt() {
    return this.#revalidateIntervalMs === undefined
      ? undefined
      : this.#now() + this.#revalidateIntervalMs;
  }
}

function intervalMs(seconds: number | undefined) {
  return seconds === undefined ? undefined : seconds * 1000;
}

function authEquals(a: Auth | undefined, b: Auth | undefined) {
  if (a === b) {
    return true;
  }
  if (!a || !b) {
    return false;
  }
  return a.type === b.type && a.raw === b.raw;
}

function snapshotConnection(
  connection: ConnectionAuthState | undefined,
): Readonly<ConnectionAuthState> | undefined {
  if (!connection) {
    return undefined;
  }
  return snapshotLiveConnection(connection);
}

function snapshotLiveConnection(
  connection: ConnectionAuthState,
): Readonly<ConnectionAuthState> {
  return {
    ...connection,
    userQueryHeaders: connection.userQueryHeaders
      ? {...connection.userQueryHeaders}
      : undefined,
  };
}

function snapshotGroup(group: GroupAuthState): Readonly<GroupAuthState> {
  return {
    ...group,
    selectedConnection: group.selectedConnection
      ? {...group.selectedConnection}
      : undefined,
  };
}

function selectorOf(connection: ConnectionAuthState): ConnectionSelector {
  return {clientID: connection.clientID, wsID: connection.wsID};
}

function compareByInsertionOrder(
  a: Pick<ConnectionAuthState, 'insertionOrder' | 'wsID'>,
  b: Pick<ConnectionAuthState, 'insertionOrder' | 'wsID'>,
) {
  return a.insertionOrder - b.insertionOrder || a.wsID.localeCompare(b.wsID);
}

function comparePreferredValidatedConnection(
  a: Pick<ConnectionAuthState, 'insertionOrder' | 'wsID'>,
  b: Pick<ConnectionAuthState, 'insertionOrder' | 'wsID'>,
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

function invalidConnectionRequest(): never {
  throw new ProtocolErrorWithLevel(
    {
      kind: ErrorKind.InvalidConnectionRequest,
      message: 'Connection auth state was not available for this websocket.',
      origin: ErrorOrigin.ZeroCache,
    },
    'warn',
  );
}

function throwPrincipalMismatch(): never {
  throw new ProtocolErrorWithLevel(
    {
      kind: ErrorKind.Unauthorized,
      message:
        'Client groups are pinned to a single principal. Connection principal does not match existing client group principal.',
      origin: ErrorOrigin.ZeroCache,
    },
    'warn',
  );
}
