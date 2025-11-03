import {type Resolver, resolver} from '@rocicorp/resolver';
import {Subscribable} from '../../../shared/src/subscribable.ts';
import {ConnectionStatus} from './connection-status.ts';
import {
  ClientError,
  isClientError,
  type AuthError,
  type ZeroError,
} from './error.ts';
import {ClientErrorKind} from './client-error-kind.ts';

const DEFAULT_TIMEOUT_CHECK_INTERVAL_MS = 1_000;

/**
 * The current connection state of the Zero instance. It can be one of the following states:
 *
 * - `connecting`: The client is actively trying to connect every 5 seconds.
 *   - `attempt` counts the number of retries within the current retry window,
 *   - `disconnectAt` is the epoch timestamp when the client will transition to `disconnected` state
 *   - `reason` is the optional error associated with the connection attempt.
 * - `disconnected`: The client is now in an "offline" state. It will continue
 *   to try to connect every 5 seconds.
 * - `connected`: The client has opened a successful connection to the server.
 * - `needs-auth`: Authentication is invalid or expired. No connection retries will be made
 *   until the host application calls `connect({auth: token})`.
 *   - `reason` is the `ZeroError` associated with the error state.
 * - `error`: A fatal error occurred. No connection retries will be made until the host
 *   application calls `connect()` again.
 *   - `reason` is the `ZeroError` associated with the error state.
 * - `closed`: The client was shut down (for example via `zero.close()`). This is
 *   a terminal state, and a new Zero instance must be created to reconnect.
 */
export type ConnectionState =
  | {
      name: ConnectionStatus.Disconnected;
      reason: ZeroError;
    }
  | {
      name: ConnectionStatus.Connecting;
      attempt: number;
      disconnectAt: number;
      reason?: ZeroError | undefined;
    }
  | {
      name: ConnectionStatus.Connected;
    }
  | {
      name: ConnectionStatus.NeedsAuth;
      reason: AuthError;
    }
  | {
      name: ConnectionStatus.Error;
      reason: ZeroError;
    }
  | {
      name: ConnectionStatus.Closed;
      reason: ZeroError;
    };

export type ConnectionManagerOptions = {
  /**
   * The amount of time we allow for continuous connecting attempts before
   * transitioning to disconnected state.
   */
  disconnectTimeoutMs: number;
  /**
   * How frequently we check whether the connecting timeout has elapsed.
   * Defaults to 1 second.
   */
  timeoutCheckIntervalMs?: number | undefined;
};

const TERMINAL_STATES = [
  ConnectionStatus.NeedsAuth,
  ConnectionStatus.Error,
] as const satisfies ConnectionStatus[];

type TerminalConnectionStatus = (typeof TERMINAL_STATES)[number];
type TerminalConnectionState = Extract<
  ConnectionState,
  {name: TerminalConnectionStatus}
>;

export class ConnectionManager extends Subscribable<ConnectionState> {
  #state: ConnectionState;

  /**
   * The timestamp when we first started trying to connect.
   * This is used to track the retry window.
   * Reset to undefined when we successfully connect or when we transition to disconnected.
   */
  #connectingStartedAt: number | undefined;

  /**
   * The amount of time we allow for continuous connecting attempts before
   * transitioning to disconnected state.
   */
  #disconnectTimeoutMs: number;

  /**
   * Handle for the timeout interval that periodically checks whether we've
   * exceeded the allowed connecting window.
   */
  #timeoutInterval: ReturnType<typeof setInterval> | undefined;

  /**
   * Interval duration for checking whether the connecting timeout has elapsed.
   */
  #timeoutCheckIntervalMs: number;

  /**
   * Resolver used to signal waiting callers when the state changes.
   */
  #stateChangeResolver: Resolver<ConnectionState> = resolver();

  constructor(options: ConnectionManagerOptions) {
    super();

    const now = Date.now();

    this.#disconnectTimeoutMs = options.disconnectTimeoutMs;
    this.#timeoutCheckIntervalMs =
      options.timeoutCheckIntervalMs ?? DEFAULT_TIMEOUT_CHECK_INTERVAL_MS;
    this.#state = {
      name: ConnectionStatus.Connecting,
      attempt: 0,
      disconnectAt: now + this.#disconnectTimeoutMs,
    };
    this.#connectingStartedAt = now;
    this.#maybeStartTimeoutInterval();
  }

  get state(): ConnectionState {
    return this.#state;
  }

  /**
   * Returns true if the current state is equal to the given status.
   */
  is(status: ConnectionStatus): boolean {
    return this.#state.name === status;
  }

  /**
   * Returns true if the current state is a terminal state
   * that can be recovered from by calling connect().
   */
  isInTerminalState(): boolean {
    return ConnectionManager.isTerminalState(this.#state);
  }

  /**
   * Returns true if the given status is a terminal state
   * that can be recovered from by calling connect().
   */
  static isTerminalState(
    state: ConnectionState,
  ): state is TerminalConnectionState {
    return (TERMINAL_STATES as readonly ConnectionStatus[]).includes(
      state.name,
    );
  }

  /**
   * Returns true if the run loop should continue.
   * The run loop continues in all states except closed.
   * In needs-auth and error states, the run loop pauses and waits for connect() to be called.
   */
  shouldContinueRunLoop(): boolean {
    return this.#state.name !== ConnectionStatus.Closed;
  }

  /**
   * Waits for the next state change.
   * @returns A promise that resolves when the next state change occurs.
   */
  waitForStateChange(): Promise<ConnectionState> {
    return this.#nextStatePromise();
  }

  /**
   * Transition to connecting state.
   *
   * This starts the 5-minute timeout timer, but if we've entered disconnected state,
   * we stay there and continue retrying.
   *
   * @returns An object containing a promise that resolves on the next state change.
   */
  connecting(reason?: ZeroError): {
    nextStatePromise: Promise<ConnectionState>;
  } {
    // cannot transition from closed to any other status
    if (this.#state.name === ConnectionStatus.Closed) {
      return {nextStatePromise: this.#nextStatePromise()};
    }

    // we cannot intentionally transition from disconnected to connecting
    // disconnected can transition to connected on successful connection
    // or a terminal state
    if (this.#state.name === ConnectionStatus.Disconnected) {
      return {nextStatePromise: this.#nextStatePromise()};
    }

    const now = Date.now();

    // If we're already connecting, increment the attempt counter
    if (this.#state.name === ConnectionStatus.Connecting) {
      this.#state = {
        ...this.#state,
        attempt: this.#state.attempt + 1,
        reason,
      };
      const nextStatePromise = this.#publishStateAndGetPromise();
      this.#maybeStartTimeoutInterval();
      return {nextStatePromise};
    }

    // Starting a new connecting session
    // If #connectingStartedAt is undefined, this is a fresh start - set it to now
    // If it's already set, we're retrying within the same retry window, so keep it
    if (this.#connectingStartedAt === undefined) {
      this.#connectingStartedAt = now;
    }

    const disconnectAt = this.#connectingStartedAt + this.#disconnectTimeoutMs;

    this.#state = {
      name: ConnectionStatus.Connecting,
      attempt: 1,
      disconnectAt,
      reason,
    };
    const nextStatePromise = this.#publishStateAndGetPromise();
    this.#maybeStartTimeoutInterval();
    return {nextStatePromise};
  }

  /**
   * Transition to connected state.
   * This resets the connecting timeout timer.
   *
   * @returns An object containing a promise that resolves on the next state change.
   */
  connected(): {nextStatePromise: Promise<ConnectionState>} {
    // cannot transition from closed to any other status
    if (this.#state.name === ConnectionStatus.Closed) {
      return {nextStatePromise: this.#nextStatePromise()};
    }

    // Already connected, no-op
    if (this.#state.name === ConnectionStatus.Connected) {
      return {nextStatePromise: this.#nextStatePromise()};
    }

    // Reset the timeout timer on successful connection
    this.#connectingStartedAt = undefined;
    this.#maybeStopTimeoutInterval();

    this.#state = {
      name: ConnectionStatus.Connected,
    };
    const nextStatePromise = this.#publishStateAndGetPromise();
    return {nextStatePromise};
  }

  /**
   * Transition to disconnected state.
   * This is called when the 5-minute timeout expires, or when we're intentionally
   * disconnecting due to an error (this will eventually be a separate state, error).
   * The run loop will continue trying to reconnect.
   *
   * @returns An object containing a promise that resolves on the next state change.
   */
  disconnected(reason: ZeroError): {
    nextStatePromise: Promise<ConnectionState>;
  } {
    // cannot transition from closed to any other status
    if (this.#state.name === ConnectionStatus.Closed) {
      return {nextStatePromise: this.#nextStatePromise()};
    }

    // Already disconnected, no-op
    if (this.#state.name === ConnectionStatus.Disconnected) {
      return {nextStatePromise: this.#nextStatePromise()};
    }

    // When transitioning from connected to disconnected, we've lost a connection
    // we previously had. Clear the timeout timer so we can start a fresh 5-minute window.
    if (this.#state.name === ConnectionStatus.Connected) {
      this.#connectingStartedAt = undefined;
    }
    // When transitioning from connecting to disconnected (e.g., due to timeout),
    // we keep the start time to maintain the context that we've been trying for a while.

    this.#maybeStopTimeoutInterval();

    this.#state = {
      name: ConnectionStatus.Disconnected,
      reason,
    };
    const nextStatePromise = this.#publishStateAndGetPromise();
    return {nextStatePromise};
  }

  /**
   * Transition to needs-auth state.
   * This pauses the run loop until connect() is called with new credentials.
   * Resets the retry window and attempt counter.
   *
   * @returns An object containing a promise that resolves on the next state change.
   */
  needsAuth(reason: AuthError): {
    nextStatePromise: Promise<ConnectionState>;
  } {
    // cannot transition from closed to any other status
    if (this.#state.name === ConnectionStatus.Closed) {
      return {nextStatePromise: this.#nextStatePromise()};
    }

    // Already in needs-auth state, no-op
    if (this.#state.name === ConnectionStatus.NeedsAuth) {
      return {nextStatePromise: this.#nextStatePromise()};
    }

    // Reset the timeout timer and connecting start time
    this.#connectingStartedAt = undefined;
    this.#maybeStopTimeoutInterval();

    this.#state = {
      name: ConnectionStatus.NeedsAuth,
      reason,
    };
    const nextStatePromise = this.#publishStateAndGetPromise();
    return {nextStatePromise};
  }

  /**
   * Transition to error state.
   * This pauses the run loop until connect() is called.
   * Resets the retry window and attempt counter.
   *
   * @returns An object containing a promise that resolves on the next state change.
   */
  error(reason: ZeroError): {nextStatePromise: Promise<ConnectionState>} {
    // cannot transition from closed to any other status
    if (this.#state.name === ConnectionStatus.Closed) {
      return {nextStatePromise: this.#nextStatePromise()};
    }

    // Already in error state, no-op
    if (this.#state.name === ConnectionStatus.Error) {
      return {nextStatePromise: this.#nextStatePromise()};
    }

    // Reset the timeout timer and connecting start time
    this.#connectingStartedAt = undefined;
    this.#maybeStopTimeoutInterval();

    this.#state = {
      name: ConnectionStatus.Error,
      reason,
    };
    const nextStatePromise = this.#publishStateAndGetPromise();
    return {nextStatePromise};
  }

  /**
   * Transition to closed state.
   * This is terminal - no further transitions are allowed.
   */
  closed() {
    // Already closed, no-op
    if (this.#state.name === ConnectionStatus.Closed) {
      return;
    }

    this.#connectingStartedAt = undefined;
    this.#maybeStopTimeoutInterval();

    this.#state = {
      name: ConnectionStatus.Closed,
      reason: new ClientError({
        kind: ClientErrorKind.ClientClosed,
        message: 'Zero was explicitly closed by calling zero.close()',
      }),
    };
    this.#publishState();
    this.cleanup();
    return;
  }

  override cleanup = (): void => {
    this._listeners.clear();
    this.#resolveNextStateWaiters();
  };

  #resolveNextStateWaiters(): void {
    this.#stateChangeResolver.resolve(this.#state);
    this.#stateChangeResolver = resolver();
  }

  #publishState(): void {
    this.notify(this.#state);
    this.#resolveNextStateWaiters();
  }

  #nextStatePromise(): Promise<ConnectionState> {
    return this.#stateChangeResolver.promise;
  }

  #publishStateAndGetPromise(): Promise<ConnectionState> {
    this.#publishState();
    return this.#nextStatePromise();
  }

  /**
   * Check if we should transition from connecting to disconnected due to timeout.
   * Returns true if the transition happened.
   */
  #checkTimeout(): boolean {
    if (this.#state.name !== ConnectionStatus.Connecting) {
      return false;
    }

    const now = Date.now();
    if (now >= this.#state.disconnectAt) {
      this.disconnected(
        new ClientError({
          kind: ClientErrorKind.DisconnectTimeout,
          message: `Zero was unable to connect for ${Math.floor(this.#disconnectTimeoutMs / 1000)} seconds and was disconnected`,
        }),
      );
      return true;
    }

    return false;
  }

  #maybeStartTimeoutInterval(): void {
    if (this.#timeoutInterval !== undefined) {
      return;
    }
    this.#timeoutInterval = setInterval(() => {
      this.#checkTimeout();
    }, this.#timeoutCheckIntervalMs);
  }

  #maybeStopTimeoutInterval(): void {
    if (this.#timeoutInterval === undefined) {
      return;
    }
    clearInterval(this.#timeoutInterval);
    this.#timeoutInterval = undefined;
  }
}

/**
 * Used to trigger the catch block when a terminal state is reached.
 *
 * @param state - The current connection state.
 */
export const throwIfConnectionError = (state: ConnectionState) => {
  if (
    ConnectionManager.isTerminalState(state) ||
    state.name === ConnectionStatus.Closed ||
    ((state.name === ConnectionStatus.Connecting ||
      state.name === ConnectionStatus.Disconnected) &&
      state.reason)
  ) {
    if (
      isClientError(state.reason) &&
      (state.reason.kind === ClientErrorKind.ConnectTimeout ||
        state.reason.kind === ClientErrorKind.AbruptClose ||
        state.reason.kind === ClientErrorKind.CleanClose)
    ) {
      return;
    }
    throw state.reason;
  }
};
