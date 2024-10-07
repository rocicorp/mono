import type {ErrorMessage} from 'zero-protocol/src/mod.js';

export class ErrorForClient extends Error {
  readonly errorMessage;
  constructor(errorMessage: ErrorMessage, options?: ErrorOptions) {
    super(JSON.stringify(errorMessage), options);
    this.errorMessage = errorMessage;
  }
}

export function findErrorForClient(error: unknown): ErrorForClient | undefined {
  if (error instanceof ErrorForClient) {
    return error;
  }
  if (error instanceof Error && error.cause) {
    return findErrorForClient(error.cause);
  }
  return undefined;
}
