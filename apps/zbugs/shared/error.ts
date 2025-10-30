import {ApplicationError} from '@rocicorp/zero';

export const MutationErrorCode = {
  NOT_LOGGED_IN: 'NOT_LOGGED_IN',
  NOT_AUTHORIZED: 'NOT_AUTHORIZED',
  NOTIFICATION_FAILED: 'NOTIFICATION_FAILED',
  ENTITY_NOT_FOUND: 'ENTITY_NOT_FOUND',
} as const;

export type MutationErrorCode =
  (typeof MutationErrorCode)[keyof typeof MutationErrorCode];

export class MutationError<
  const T extends MutationErrorCode = MutationErrorCode,
> extends ApplicationError<{
  code: T;
  id: string | undefined;
}> {
  constructor(message: string, code: T, id?: string) {
    super(message, {
      details: {code, id},
    });
  }
}

export const QueryErrorCode = {
  UNKNOWN_FILTER: 'UNKNOWN_FILTER',
  UNKNOWN_QUERY: 'UNKNOWN_QUERY',
} as const;

export type QueryErrorCode =
  (typeof QueryErrorCode)[keyof typeof QueryErrorCode];

export class QueryError<
  const T extends QueryErrorCode = QueryErrorCode,
> extends ApplicationError<{
  code: T;
  filter: string | undefined;
}> {
  constructor(message: string, code: T, filter?: string) {
    super(message, {
      details: {code, filter},
    });
  }
}
