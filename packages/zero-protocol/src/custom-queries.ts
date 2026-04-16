import {jsonSchema} from '../../shared/src/json-schema.ts';
import * as v from '../../shared/src/valita.ts';
import {astSchema} from './ast.ts';
import {transformFailedBodySchema} from './error.ts';

/* Shared payloads */

export const transformRequestBodySchema = v.array(
  v.object({
    id: v.string(),
    name: v.string(),
    args: v.readonly(v.array(jsonSchema)),
  }),
);
export type TransformRequestBody = v.Infer<typeof transformRequestBodySchema>;

export const transformedQuerySchema = v.object({
  id: v.string(),
  name: v.string(),
  ast: astSchema,
});

export const appErroredQuerySchema = v.object({
  error: v.literal('app'),
  id: v.string(),
  name: v.string(),
  // optional for backwards compatibility
  message: v.string().optional(),
  details: jsonSchema.optional(),
});
export const parseErroredQuerySchema = v.object({
  error: v.literal('parse'),
  id: v.string(),
  name: v.string(),
  message: v.string(),
  details: jsonSchema.optional(),
});
export const erroredQuerySchema = v.union(
  appErroredQuerySchema,
  parseErroredQuerySchema,
);
export type ErroredQuery = v.Infer<typeof erroredQuerySchema>;

export const queryResultSchema = v.union(
  transformedQuerySchema,
  erroredQuerySchema,
);
export type QueryResult = v.Infer<typeof queryResultSchema>;

export const queryResponseBodySchema = v.array(queryResultSchema);
export type QueryResponseBody = v.Infer<typeof queryResponseBodySchema>;

/* Legacy API */

export const transformRequestMessageSchema = v.tuple([
  v.literal('transform'),
  transformRequestBodySchema,
]);
export type TransformRequestMessage = v.Infer<
  typeof transformRequestMessageSchema
>;

const transformFailedErrorMessageSchema = v.tuple([
  v.literal('transformFailed'),
  transformFailedBodySchema,
]);
const legacyTransformSuccessMessageSchema = v.tuple([
  v.literal('transformed'),
  queryResponseBodySchema,
]);

export const transformResponseMessageSchema = v.union(
  legacyTransformSuccessMessageSchema,
  transformFailedErrorMessageSchema,
);
export type TransformResponseMessage = v.Infer<
  typeof transformResponseMessageSchema
>;

export const legacyTransformResponseMessageSchema =
  transformResponseMessageSchema;
export type LegacyTransformResponseMessage = TransformResponseMessage;

export const transformResponseBodySchema = queryResponseBodySchema;
export type TransformResponseBody = QueryResponseBody;

/* API /query */

export const querySuccessSchema = v.object({
  kind: v.literal('QueryResponse'),
  userID: v.string().nullable(),
  queries: queryResponseBodySchema,
});
export type QuerySuccess = v.Infer<typeof querySuccessSchema>;

export const queryResponseSchema = v.union(
  querySuccessSchema,
  transformFailedBodySchema,
);
export type QueryResponse = v.Infer<typeof queryResponseSchema>;

export const apiQueryResponseSchema = v.union(
  queryResponseSchema,
  transformResponseMessageSchema,
);
export type APIQueryResponse = v.Infer<typeof apiQueryResponseSchema>;

/* Zero client <-> Zero cache */

export const transformErrorMessageSchema = v.tuple([
  v.literal('transformError'),
  v.array(erroredQuerySchema),
]);
export type TransformErrorMessage = v.Infer<typeof transformErrorMessageSchema>;
