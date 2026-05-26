import type {LogContext} from '@rocicorp/logger';
import {unreachable} from '../../../shared/src/asserts.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {ApplicationError} from '../../../zero-protocol/src/application-error.ts';
import {wrapWithApplicationError} from '../../../zero-protocol/src/application-error.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import type {
  ConnectionManager,
  ConnectionManagerState,
} from './connection-manager.ts';
import {ConnectionStatus} from './connection-status.ts';
import type {
  MutatorResult,
  MutatorResultErrorDetails,
  MutatorResultSuccessDetails,
} from './custom.ts';
import {isZeroError, type ZeroError} from './error.ts';
import type {MutationTracker} from './mutation-tracker.ts';

const successResultDetails = {
  type: 'success',
} as const satisfies MutatorResultSuccessDetails;
const successResult = () => successResultDetails;

function getStateDescription(error: ZeroError): string {
  switch (error.kind) {
    case ClientErrorKind.Offline:
      return 'offline';
    case ClientErrorKind.ClientClosed:
      return 'closed';
    default:
      return 'in error state';
  }
}

type CachedMutationRejection = {
  readonly error: ZeroError;
  readonly promise: Promise<MutatorResultErrorDetails>;
};

export class MutatorProxy {
  readonly #lc: LogContext;
  readonly #connectionManager: ConnectionManager;
  readonly #mutationTracker: MutationTracker;
  #mutationRejection: CachedMutationRejection | undefined;

  constructor(
    lc: LogContext,
    connectionManager: ConnectionManager,
    mutationTracker: MutationTracker,
  ) {
    this.#lc = lc;
    this.#connectionManager = connectionManager;
    this.#mutationTracker = mutationTracker;

    this.#connectionManager.subscribe(state =>
      this.#onConnectionStateChange(state),
    );
    this.#onConnectionStateChange(connectionManager.state);
  }

  get mutationRejectionError(): ZeroError | undefined {
    return this.#mutationRejection?.error;
  }

  /**
   * Called when the connection state changes.
   *
   * If the connection state is disconnected, error, or closed, the
   * mutation rejection error is set and all outstanding `.server` promises in
   * the mutation tracker are rejected with the error.
   */
  #onConnectionStateChange(state: ConnectionManagerState) {
    // we short circuit the rejection if the error is due to a missing cacheURL
    // this allows local writes to continue
    if (
      state.name === ConnectionStatus.Disconnected &&
      state.reason.kind === ClientErrorKind.NoSocketOrigin
    ) {
      this.#mutationRejection = undefined;
      return;
    }

    switch (state.name) {
      case ConnectionStatus.Disconnected:
      case ConnectionStatus.Error:
      case ConnectionStatus.Closed:
        this.#mutationRejection = {
          error: state.reason,
          promise: Promise.resolve(
            this.#makeZeroErrorResultDetails(state.reason),
          ),
        };
        this.#mutationTracker.rejectAllOutstandingMutations(state.reason);
        break;
      case ConnectionStatus.Connected:
      case ConnectionStatus.Connecting:
      case ConnectionStatus.NeedsAuth:
        this.#mutationRejection = undefined;
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
  >(name: string, f: F): (...args: Parameters<F>) => MutatorResult {
    return (...args) => {
      if (this.#mutationRejection) {
        const error = this.#mutationRejection.error;
        this.#lc.warn?.(
          `Mutation "${name}" rejected because Zero is ${getStateDescription(error)}. Details: ${error.message}. See also: https://zero.rocicorp.dev/docs/connection.`,
        );
        return {
          client: this.#mutationRejection.promise,
          server: this.#mutationRejection.promise,
        } as const satisfies MutatorResult;
      }

      let result: {
        client: Promise<unknown>;
        server: Promise<unknown>;
      };

      const cachedMutationPromises: Partial<
        Record<'client' | 'server', Promise<MutatorResultErrorDetails>>
      > = {};

      const wrapErrorFor =
        (origin: 'client' | 'server') =>
        (error: unknown): Promise<MutatorResultErrorDetails> => {
          const cachedPromise = cachedMutationPromises[origin];
          if (cachedPromise) {
            return cachedPromise;
          }

          if (isZeroError(error)) {
            this.#lc.error?.(`Mutator "${name}" error on ${origin}`, error);

            const zeroErrorPromise = this.#makeZeroErrorResultDetails(error);
            cachedMutationPromises[origin] = zeroErrorPromise;
            return zeroErrorPromise;
          }

          const applicationError = wrapWithApplicationError(error);
          this.#lc.error?.(
            `Mutator "${name}" app error on ${origin}`,
            applicationError,
          );

          const applicationErrorPromise =
            this.#makeApplicationErrorResultDetails(applicationError);
          cachedMutationPromises[origin] = applicationErrorPromise;
          return applicationErrorPromise;
        };

      try {
        result = f(...args);
      } catch (error) {
        const clientPromise = wrapErrorFor('client')(error);
        const serverPromise = wrapErrorFor('server')(error);

        return {
          client: clientPromise,
          server: serverPromise,
        } as const satisfies MutatorResult;
      }

      const client = this.#normalizeResultPromise(
        result.client,
        wrapErrorFor('client'),
      );
      const server = this.#normalizeResultPromise(
        result.server,
        wrapErrorFor('server'),
      );

      return {
        client,
        server,
      };
    };
  }

  #normalizeResultPromise(
    promise: Promise<unknown>,
    wrapError: (error: unknown) => Promise<MutatorResultErrorDetails>,
  ) {
    return promise.then<MutatorResultSuccessDetails, MutatorResultErrorDetails>(
      successResult,
      wrapError,
    );
  }

  #makeZeroErrorResultDetails(zeroError: ZeroError) {
    return Promise.resolve({
      type: 'error',
      error: {
        type: 'zero',
        message: zeroError.message,
      },
    } as const satisfies MutatorResultErrorDetails);
  }

  #makeApplicationErrorResultDetails(applicationError: ApplicationError) {
    return Promise.resolve({
      type: 'error',
      error: {
        type: 'app',
        message: applicationError.message,
        details: applicationError.details,
      },
    } as const satisfies MutatorResultErrorDetails);
  }
}
