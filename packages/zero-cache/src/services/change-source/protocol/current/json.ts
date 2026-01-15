import * as v from '@badrap/valita';

export type JSONValue =
  | null
  | string
  | boolean
  | number
  | Array<JSONValue>
  | JSONObject;

export type JSONObject = {[key: string]: JSONValue | undefined};

export const jsonValueSchema: v.Type<JSONValue> = v.lazy(() =>
  v.union(
    v.null(),
    v.string(),
    v.boolean(),
    v.number(),
    v.array(jsonValueSchema),
    jsonObjectSchema,
  ),
);

export const jsonObjectSchema = v.record(
  v.union(jsonValueSchema, v.undefined()),
);
