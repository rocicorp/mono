import {jsonSchema} from '../../shared/src/json-schema.ts';
import * as v from '../../shared/src/valita.ts';
import {astSchema} from './ast.ts';
import {transformFailedBodySchema} from './error.ts';

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

export const appQueryErrorSchema = v.object({
  error: v.literal('app'),
  id: v.string(),
  name: v.string(),
  details: jsonSchema,
});

/** @deprecated zero errors are now represented as ['error', { ... }] messages */
export const zeroErrorSchema = v.object({
  /** @deprecated */
  error: v.literal('zero'),
  /** @deprecated */
  id: v.string(),
  /** @deprecated */
  name: v.string(),
  /** @deprecated */
  details: jsonSchema,
});
/** @deprecated http errors are now represented as ['error', { ... }] messages */
export const httpQueryErrorSchema = v.object({
  /** @deprecated */
  error: v.literal('http'),
  /** @deprecated */
  id: v.string(),
  /** @deprecated */
  name: v.string(),
  /** @deprecated */
  status: v.number(),
  /** @deprecated */
  details: jsonSchema,
});

export const erroredQuerySchema = v.union(
  appQueryErrorSchema,
  httpQueryErrorSchema,
  zeroErrorSchema,
);
export type ErroredQuery = v.Infer<typeof erroredQuerySchema>;

export const transformResponseBodySchema = v.array(
  v.union(transformedQuerySchema, erroredQuerySchema),
);
export type TransformResponseBody = v.Infer<typeof transformResponseBodySchema>;

export const transformRequestMessageSchema = v.tuple([
  v.literal('transform'),
  transformRequestBodySchema,
]);
export type TransformRequestMessage = v.Infer<
  typeof transformRequestMessageSchema
>;
export const transformErrorMessageSchema = v.tuple([
  v.literal('transformError'),
  v.array(erroredQuerySchema),
]);
export type TransformErrorMessage = v.Infer<typeof transformErrorMessageSchema>;

const transformFailedMessageSchema = v.tuple([
  v.literal('transformFailed'),
  transformFailedBodySchema,
]);
const transformOkMessageSchema = v.tuple([
  v.literal('transformed'),
  transformResponseBodySchema,
]);

export const transformResponseMessageSchema = v.union(
  transformOkMessageSchema,
  transformFailedMessageSchema,
);
export type TransformResponseMessage = v.Infer<
  typeof transformResponseMessageSchema
>;
