import type {LogContext} from '@rocicorp/logger';
import type {ConnectionManager, ConnectionState} from './connection-manager.ts';

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

  async connect(opts?: {auth: string | null | undefined}): Promise<void> {
    const lc = this.#lc.withContext('connect');

    if (opts && 'auth' in opts) {
      lc.debug?.('Updating auth credential from connect()');
      this.#setAuth(opts.auth);
    }

    // only allow connect() to be called from a terminal state
    if (!this.#connectionManager.isInTerminalState()) {
      lc.debug?.(
        'connect() called but not in a terminal state. Current state:',
        this.#connectionManager.state.name,
      );
      return;
    }

    lc.info?.(
      `Resuming connection from state: ${this.#connectionManager.state.name}`,
    );

    // Transition to connecting, which will trigger the state change resolver
    // and unblock the run loop. Wait for the next state change (connected, disconnected, etc.)
    const {nextStatePromise} = this.#connectionManager.connecting();
    await nextStatePromise;
  }
}

export class ConnectionSource implements Source<ConnectionState> {
  readonly #connectionManager: ConnectionManager;

  constructor(connectionManager: ConnectionManager) {
    this.#connectionManager = connectionManager;
  }

  get current(): ConnectionState {
    return this.#connectionManager.state;
  }

  get subscribe() {
    return this.#connectionManager.subscribe;
  }
}
