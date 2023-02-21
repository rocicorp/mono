import * as s from 'superstruct';

type Literal = boolean | null | number | string;
type Json = Literal | {[key: string]: Json} | Json[];
const literalSchema = s.union([
  s.string(),
  s.number(),
  s.boolean(),
  s.literal(null),
]);
export const jsonSchema: s.Struct<Json> = s.lazy(() =>
  s.union([
    literalSchema,
    s.array(jsonSchema),
    s.record(s.string(), jsonSchema),
  ]),
);
export type JSONType = s.Infer<typeof jsonSchema>;
