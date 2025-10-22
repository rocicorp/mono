import {type Resolver, resolver} from '@rocicorp/resolver';
import {Subscribable} from '../../../shared/src/subscribable.ts';
import {ConnectionStatus} from './connection-status.ts';
import type {ZeroError} from './error.ts';

const DEFAULT_TIMEOUT_CHECK_INTERVAL_MS = 1_000;

export type ConnectionState =
  | {
      name: ConnectionStatus.Disconnected;
      reason?: ZeroError | undefined;
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
      name: ConnectionStatus.Closed;
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
  #stateChangeResolver: Resolver<void> = resolver();

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
   * Returns true if the run loop should continue.
   * The run loop continues in disconnected, connecting, and connected states.
   * It stops in closed state.
   */
  shouldContinueRunLoop(): boolean {
    return this.#state.name !== ConnectionStatus.Closed;
  }

  waitForStateChange(): Promise<void> {
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
  connecting(reason?: ZeroError | undefined): {
    nextStatePromise: Promise<void>;
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
  connected(): {nextStatePromise: Promise<void>} {
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
  disconnected(reason?: ZeroError | undefined): {
    nextStatePromise: Promise<void>;
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
   * Transition to closed state.
   * This is terminal - no further transitions are allowed.
   *
   * @returns An object containing a promise that resolves on the next state change.
   */
  closed(): {nextStatePromise: Promise<void>} {
    // Already closed, no-op
    if (this.#state.name === ConnectionStatus.Closed) {
      return {nextStatePromise: this.#nextStatePromise()};
    }

    this.#connectingStartedAt = undefined;
    this.#maybeStopTimeoutInterval();

    this.#state = {
      name: ConnectionStatus.Closed,
    };
    const nextStatePromise = this.#publishStateAndGetPromise();
    return {nextStatePromise};
  }

  override cleanup = (): void => {
    this._listeners.clear();
    this.#resolveNextStateWaiters();
  };

  #resolveNextStateWaiters(): void {
    this.#stateChangeResolver.resolve();
    this.#stateChangeResolver = resolver();
  }

  #publishState(): void {
    this.notify(this.#state);
    this.#resolveNextStateWaiters();
  }

  #nextStatePromise(): Promise<void> {
    return this.#stateChangeResolver.promise;
  }

  #publishStateAndGetPromise(): Promise<void> {
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
      this.disconnected();
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
