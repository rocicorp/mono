import {unreachable} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {
  ApplicationError,
  wrapWithApplicationError,
} from '../../../zero-protocol/src/application-error.ts';
import type {ConnectionManager, ConnectionState} from './connection-manager.ts';
import {ConnectionStatus} from './connection-status.ts';
import type {MutatorResult, MutatorResultDetails} from './custom.ts';
import {type ZeroError} from './error.ts';
import type {MutationTracker} from './mutation-tracker.ts';

const successResultDetails: MutatorResultDetails = {type: 'success'};

export class MutatorProxy {
  readonly #connectionManager: ConnectionManager;
  readonly #mutationTracker: MutationTracker;
  #mutationRejectionError: ZeroError | undefined;

  readonly #onApplicationError: (error: ApplicationError) => void;

  constructor(
    connectionManager: ConnectionManager,
    mutationTracker: MutationTracker,
    onApplicationError: (error: ApplicationError) => void,
  ) {
    this.#connectionManager = connectionManager;
    this.#mutationTracker = mutationTracker;
    this.#onApplicationError = onApplicationError;

    this.#connectionManager.subscribe(state =>
      this.#onConnectionStateChange(state),
    );
  }

  get mutationRejectionError(): ZeroError | undefined {
    return this.#mutationRejectionError;
  }

  /**
   * Called when the connection state changes.
   *
   * If the connection state is disconnected, error, or closed, the
   * mutation rejection error is set and all outstanding `.server` promises in
   * the mutation tracker are rejected with the error.
   */
  #onConnectionStateChange(state: ConnectionState) {
    switch (state.name) {
      case ConnectionStatus.Disconnected:
      case ConnectionStatus.Error:
      case ConnectionStatus.Closed:
        this.#mutationRejectionError = state.reason;
        this.#mutationTracker.rejectAllOutstandingMutations(state.reason);
        break;
      case ConnectionStatus.Connected:
      case ConnectionStatus.Connecting:
      case ConnectionStatus.NeedsAuth:
        this.#mutationRejectionError = undefined;
        return;
      default:
        unreachable(state);
    }
  }

  wrapCustomMutator<
    F extends (...args: [] | [ReadonlyJSONValue]) => {
      client: Promise<unknown>;
      server: Promise<unknown>;
    },
  >(f: F): (...args: Parameters<F>) => MutatorResult {
    return (...args) => {
      if (this.#mutationRejectionError) {
        const errorDetails = this.#wrapWithZeroError(
          this.#mutationRejectionError,
        );
        return {
          client: Promise.resolve(errorDetails),
          server: Promise.resolve(errorDetails),
        } as const satisfies MutatorResult;
      }

      let result: {
        client: Promise<unknown>;
        server: Promise<unknown>;
      };
      try {
        result = f(...args);
      } catch (error) {
        const errorDetails = this.#wrapWithApplicationError(error);
        return {
          client: Promise.resolve(errorDetails),
          server: Promise.resolve(errorDetails),
        } as const satisfies MutatorResult;
      }

      const client = result.client
        .then(result =>
          result && typeof result === 'object' && 'type' in result
            ? (result as MutatorResultDetails)
            : successResultDetails,
        )
        .catch(error => this.#wrapWithApplicationError(error));
      const server = result.server
        .then(result =>
          result && typeof result === 'object' && 'type' in result
            ? (result as MutatorResultDetails)
            : successResultDetails,
        )
        .catch(error => this.#wrapWithApplicationError(error));

      return {
        client,
        server,
      };
    };
  }

  #wrapWithApplicationError(error: unknown): MutatorResultDetails {
    const wrappedError = wrapWithApplicationError(error);
    this.#onApplicationError(wrappedError);

    return {
      type: 'error',
      error: {
        type: 'app',
        message: wrappedError.message,
        details: wrappedError.details,
      },
    } as const satisfies MutatorResultDetails;
  }

  #wrapWithZeroError(error: ZeroError): MutatorResultDetails {
    const {message, ...errorBody} = error.errorBody;

    return {
      type: 'error',
      error: {
        type: 'zero',
        message,
        details: errorBody,
      },
    } as const satisfies MutatorResultDetails;
  }
}
