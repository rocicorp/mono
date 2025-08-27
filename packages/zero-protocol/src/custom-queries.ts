import {jsonSchema} from '../../shared/src/json-schema.ts';
import * as v from '../../shared/src/valita.ts';
import {astSchema} from './ast.ts';

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

export const zeroErrorSchema = v.object({
  error: v.literal('zero'),
  id: v.string(),
  name: v.string(),
  details: jsonSchema,
});

export const httpQueryErrorSchema = v.object({
  error: v.literal('http'),
  id: v.string(),
  name: v.string(),
  status: v.number(),
  details: jsonSchema,
});

export const erroredQuerySchema = v.union(
  appQueryErrorSchema,
  httpQueryErrorSchema,
  zeroErrorSchema,
);
export type ErroredQuery = v.Infer<typeof erroredQuerySchema>;
export type AppQueryError = v.Infer<typeof appQueryErrorSchema>;
export type HttpQueryError = v.Infer<typeof httpQueryErrorSchema>;

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

export const transformResponseMessageSchema = v.tuple([
  v.literal('transformed'),
  transformResponseBodySchema,
]);
export type TransformResponseMessage = v.Infer<
  typeof transformResponseMessageSchema
>;
