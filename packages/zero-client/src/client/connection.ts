import type {LogContext} from '@rocicorp/logger';
import {unreachable} from '../../../shared/src/asserts.ts';
import {Subscribable} from '../../../shared/src/subscribable.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import type {
  ConnectionManager,
  ConnectionManagerState,
} from './connection-manager.ts';
import {ConnectionStatus} from './connection-status.ts';

/**
 * The current connection state of the Zero instance. One of the following states:
 *
 * - `connecting`: The client is actively trying to connect every 5 seconds.
 * - `disconnected`: The client is now in an "offline" state. It will continue
 *   to try to connect every 5 seconds.
 * - `connected`: The client has opened a successful connection to the server.
 * - `needs-auth`: Authentication is invalid or expired. No connection retries will be made
 *   until the host application calls `connect()`.
 * - `error`: A fatal error occurred. No connection retries will be made until the host
 *   application calls `connect()` again.
 * - `closed`: The client was shut down (for example via `zero.close()`). This is
 *   a terminal state, and a new Zero instance must be created to reconnect.
 */
export type ConnectionState =
  | {
      name: 'disconnected';
      reason: string;
    }
  | {
      name: 'connecting';
      reason?: string;
    }
  | {
      name: 'connected';
    }
  | {
      name: 'needs-auth';
      reason:
        | {
            type: 'mutate';
            status: 401 | 403;
            body?: string;
          }
        | {
            type: 'query';
            status: 401 | 403;
            body?: string;
          }
        | {
            type: 'zero-cache';
            reason: string;
          };
    }
  | {
      name: 'error';
      reason: string;
    }
  | {
      name: 'closed';
      reason: string;
    };

export interface Source<T> {
  /**
   * The current state value.
   */
  readonly current: T;

  /**
   * Subscribe to state changes.
   *
   * @param listener - Called when the state changes with the new state value.
   * @returns A function to unsubscribe from state changes.
   */
  subscribe(listener: (state: T) => void): () => void;
}

/**
 * Connection API for managing Zero's connection lifecycle.
 */
export interface Connection {
  /**
   * The current connection state as a subscribable value.
   */
  readonly state: Source<ConnectionState>;

  /**
   * Resumes the connection from a terminal state.
   *
   * If called when not in a terminal state, this method does nothing.
   *
   * @param opts - Optional connection options
   * @param opts.auth - Token to use for authentication. If provided, this overrides
   *                    the stored auth credential for this connection attempt.
   *                    If `null` or `undefined`, the stored auth credential is cleared.
   * @returns A promise that resolves once the connection state has transitioned to connecting.
   */
  connect(opts?: {auth: string | null | undefined}): Promise<void>;
}

export class ConnectionImpl implements Connection {
  readonly #connectionManager: ConnectionManager;
  readonly #lc: LogContext;
  readonly #source: ConnectionSource;
  readonly #setAuth: (auth: string | null | undefined) => void;

  constructor(
    connectionManager: ConnectionManager,
    lc: LogContext,
    setAuth: (auth: string | null | undefined) => void,
  ) {
    this.#connectionManager = connectionManager;
    this.#lc = lc;
    this.#source = new ConnectionSource(connectionManager);
    this.#setAuth = setAuth;
  }

  get state(): Source<ConnectionState> {
    return this.#source;
  }

  connect(opts?: {auth: string | null | undefined}): Promise<void> {
    const lc = this.#lc.withContext('connect');

    if (opts && 'auth' in opts) {
      lc.debug?.('Updating auth credential from connect()');
      this.#setAuth(opts.auth);
    }

    // if the connection is disconnected due to a missing cacheURL, we don't allow a reconnect
    if (
      this.#connectionManager.state.name === ConnectionStatus.Disconnected &&
      this.#connectionManager.state.reason.kind ===
        ClientErrorKind.NoSocketOrigin
    ) {
      lc.error?.(
        'connect() called but the connection is disconnected due to a missing cacheURL. No reconnect will be attempted.',
      );
      return Promise.resolve();
    }

    // only allow connect() to be called from a terminal state
    if (!this.#connectionManager.isInTerminalState()) {
      lc.debug?.(
        'connect() called but not in a terminal state. Current state:',
        this.#connectionManager.state.name,
      );
      return Promise.resolve();
    }

    lc.info?.(
      `Resuming connection from state: ${this.#connectionManager.state.name}`,
    );

    // Transition to connecting to unblock the run loop.
    this.#connectionManager.connecting();
    return Promise.resolve();
  }
}

export class ConnectionSource
  extends Subscribable<ConnectionState>
  implements Source<ConnectionState>
{
  #state: ConnectionState;

  constructor(connectionManager: ConnectionManager) {
    super();
    this.#state = this.#mapConnectionManagerState(connectionManager.state);

    // Subscribe to ConnectionManager immediately to keep #state in sync.
    // This ensures `current` always returns the correct state, even if
    // external code hasn't subscribed yet (fixes race condition where
    // connection completes before React subscribes).
    connectionManager.subscribe(state => {
      this.#state = this.#mapConnectionManagerState(state);
      this.notify(this.#state);
    });
  }

  get current(): ConnectionState {
    return this.#state;
  }

  #mapConnectionManagerState(state: ConnectionManagerState): ConnectionState {
    switch (state.name) {
      case ConnectionStatus.Closed:
        return {
          name: 'closed',
          reason: state.reason.message,
        };
      case ConnectionStatus.Connected:
        return {
          name: 'connected',
        };
      case ConnectionStatus.Connecting:
        return {
          name: 'connecting',
          ...(state.reason?.message ? {reason: state.reason.message} : {}),
        };
      case ConnectionStatus.Disconnected:
        return {
          name: 'disconnected',
          reason: state.reason.message,
        };
      case ConnectionStatus.Error:
        return {
          name: 'error',
          reason: state.reason.message,
        };
      case ConnectionStatus.NeedsAuth:
        return {
          name: 'needs-auth',
          reason:
            state.reason.errorBody.kind === ErrorKind.PushFailed
              ? {
                  type: 'mutate',
                  status: state.reason.errorBody.status,
                  ...(state.reason.errorBody.bodyPreview
                    ? {body: state.reason.errorBody.bodyPreview}
                    : {}),
                }
              : state.reason.errorBody.kind === ErrorKind.TransformFailed
                ? {
                    type: 'query',
                    status: state.reason.errorBody.status,
                    ...(state.reason.errorBody.bodyPreview
                      ? {body: state.reason.errorBody.bodyPreview}
                      : {}),
                  }
                : {
                    type: 'zero-cache',
                    reason: state.reason.message,
                  },
        };

      default:
        unreachable(state);
    }
  }
}
