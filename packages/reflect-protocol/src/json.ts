import * as v from '@badrap/valita';

type Literal = boolean | null | number | string;
type Json = Literal | {[key: string]: Json} | Json[];
const literalSchema = v.union(v.string(), v.number(), v.boolean(), v.null());

export type JSONType = v.Infer<typeof jsonSchema>;

export const jsonSchema: v.Type<Json> = v.lazy(() =>
  v.union(literalSchema, v.array(jsonSchema), v.record(jsonSchema)),
);

/**
 * A JSON value that allows undefined values in objects.
 */
export type RelaxedJSONValue =
  | boolean
  | null
  | number
  | string
  | RelaxedJSONObject
  | RelaxedJSONArray;

type RelaxedJSONArray = Array<RelaxedJSONValue>;

type RelaxedJSONObject = {[key: string]: RelaxedJSONValue | undefined};
