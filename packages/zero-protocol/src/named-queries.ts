import {jsonSchema} from '../../shared/src/json-schema.ts';
import * as v from '../../shared/src/valita.ts';
import {astSchema} from './ast.ts';

export const transformRequestBodySchema = v.array(
  v.object({
    id: v.string(),
    name: v.string(),
    args: v.array(jsonSchema),
  }),
);

export const transformedQuerySchema = v.object({
  id: v.string(),
  name: v.string(),
  ast: astSchema,
});

export const erroredQuerySchema = v.object({
  error: v.literal('app'),
  id: v.string(),
  name: v.string(),
  details: jsonSchema,
});

export const transformResponseBodySchema = v.array(
  v.union(transformedQuerySchema, erroredQuerySchema),
);

export const transformRequestMessageSchema = v.tuple([
  v.literal('transform'),
  transformRequestBodySchema,
]);
export const transformResponseMessageSchema = v.tuple([
  v.literal('transformed'),
  transformResponseBodySchema,
]);
