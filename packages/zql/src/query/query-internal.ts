/* eslint-disable @typescript-eslint/unbound-method, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-empty-object-type, @typescript-eslint/no-base-to-string */
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {PullRow, Query} from './query.ts';

/** @deprecated Use Query instead */
export interface AdvancedQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
> extends Query<TSchema, TTable, TReturn> {}
