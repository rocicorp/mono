import * as v from '../../shared/src/valita.ts';
import {
  erroredQuerySchema,
  transformedQuerySchema,
  transformResponseMessageSchema,
} from './custom-queries.ts';
import {transformFailedBodySchema} from './error.ts';

export const queryResultSchema = v.union(
  transformedQuerySchema,
  erroredQuerySchema,
);
export type QueryResult = v.Infer<typeof queryResultSchema>;

export const queryResponseBodySchema = v.array(queryResultSchema);
export type QueryResponseBody = v.Infer<typeof queryResponseBodySchema>;

export const querySuccessSchema = v.object({
  kind: v.literal('QueryResponse'),
  userID: v.string().nullable().optional(),
  queries: queryResponseBodySchema,
});
export type QuerySuccess = v.Infer<typeof querySuccessSchema>;

export const queryResponseSchema = v.union(
  querySuccessSchema,
  transformFailedBodySchema,
  // for backwards compatibility
  transformResponseMessageSchema,
);
export type QueryResponse = v.Infer<typeof queryResponseSchema>;
