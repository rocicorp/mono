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

/** @deprecated remove TODO(0xcadams): add a protocol version */
export const zeroErrorSchema = v.object({
  /** @deprecated remove TODO(0xcadams): add a protocol version */
  error: v.literal('zero'),
  /** @deprecated remove TODO(0xcadams): add a protocol version */
  id: v.string(),
  /** @deprecated remove TODO(0xcadams): add a protocol version */
  name: v.string(),
  /** @deprecated remove TODO(0xcadams): add a protocol version */
  details: jsonSchema,
});
/** @deprecated remove TODO(0xcadams): add a protocol version */
export const httpQueryErrorSchema = v.object({
  /** @deprecated remove TODO(0xcadams): add a protocol version */
  error: v.literal('http'),
  /** @deprecated remove TODO(0xcadams): add a protocol version */
  id: v.string(),
  /** @deprecated remove TODO(0xcadams): add a protocol version */
  name: v.string(),
  /** @deprecated remove TODO(0xcadams): add a protocol version */
  status: v.number(),
  /** @deprecated remove TODO(0xcadams): add a protocol version */
  details: jsonSchema,
});

export const erroredQuerySchema = v.union(
  appQueryErrorSchema,
  httpQueryErrorSchema,
  zeroErrorSchema,
);
export type AppQueryError = v.Infer<typeof erroredQuerySchema>;

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
