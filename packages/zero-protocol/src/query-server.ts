import * as v from '../../shared/src/valita.ts';
import {
  erroredQuerySchema,
  transformedQuerySchema,
  transformResponseMessageSchema,
} from './custom-queries.ts';
import {transformFailedBodySchema} from './error.ts';

const queryResultSchema = v.union(transformedQuerySchema, erroredQuerySchema);

const queryResponseBodySchema = v.array(queryResultSchema);
export type QueryResponseBody = v.Infer<typeof queryResponseBodySchema>;

const querySuccessSchema = v.object({
  kind: v.literal('QueryResponse'),
  userID: v.string().nullable().optional(),
  queries: queryResponseBodySchema,
});

export const queryResponseSchema = v.union(
  querySuccessSchema,
  transformFailedBodySchema,
  // for backwards compatibility
  transformResponseMessageSchema,
);
export type QueryResponse = v.Infer<typeof queryResponseSchema>;
