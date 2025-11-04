import {getErrorDetails, getErrorMessage} from '../../shared/src/error.ts';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';

/**
 * Options accepted by {@link ApplicationError}.
 *
 * Use these when you need to attach additional context to an error that will
 * be sent back to the client.
 */
export interface ApplicationErrorOptions<
  T extends ReadonlyJSONValue | undefined,
> {
  details?: T;
  cause?: unknown;
}

/**
 * Error type that application code can throw to surface structured metadata back to
 * the client.
 *
 * Use this when you want to return a descriptive message along with the
 * JSON-serializable `details`.
 */
export class ApplicationError<
  const T extends ReadonlyJSONValue | undefined = ReadonlyJSONValue | undefined,
> extends Error {
  /**
   * This maps onto errors for transform and push app-level failures.
   */
  readonly #details: T;

  constructor(
    message: string,
    options?: ApplicationErrorOptions<T> | undefined,
  ) {
    super(message, {cause: options?.cause});
    this.name = 'ApplicationError';
    this.#details = options?.details ?? (undefined as T);
  }

  get details(): T {
    return this.#details;
  }

  get kind(): 'Application' {
    return 'Application';
  }
}

export function isApplicationError(error: unknown): error is ApplicationError {
  return error instanceof ApplicationError;
}

export function wrapWithApplicationError<
  T extends ReadonlyJSONValue | undefined = ReadonlyJSONValue | undefined,
>(error: unknown): ApplicationError<T> {
  if (isApplicationError(error)) {
    return error as ApplicationError<T>;
  }

  const message = getErrorMessage(error);
  const details = getErrorDetails(error);

  return new ApplicationError<T>(message, {
    cause: error,
    details: details as T,
  });
}
