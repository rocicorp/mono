import type {ErrorBody} from '../../../zero-protocol/src/error.js';

export class ErrorForClient extends Error {
  readonly errorBody;
  constructor(errorBody: ErrorBody, options?: ErrorOptions) {
    super(JSON.stringify(errorBody), options);
    this.errorBody = errorBody;
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
