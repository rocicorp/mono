import {jsonSchema} from '../../shared/src/json-schema.ts';
import * as v from '../../shared/src/valita.ts';
import {astSchema} from './ast.ts';

const inspectQueryRowSchema = v.object({
  clientID: v.string(),
  queryID: v.string(),
  // null for custom queries
  ast: astSchema.nullable(),
  // not null for custom queries
  name: v.string().nullable(),
  // not null for custom queries
  args: v.readonlyArray(jsonSchema).nullable(),
  got: v.boolean(),
  deleted: v.boolean(),
  ttl: v.number(),
  inactivatedAt: v.number().nullable(),
  rowCount: v.number(),
});

export type InspectQueryRow = v.Infer<typeof inspectQueryRowSchema>;

export const inspectQueriesDownSchema = v.object({
  op: v.literal('queries'),
  id: v.string(),
  value: v.array(inspectQueryRowSchema),
});

export type InspectQueriesDown = v.Infer<typeof inspectQueriesDownSchema>;

export const inspectDownBodySchema = v.union(inspectQueriesDownSchema);

export const inspectDownMessageSchema = v.tuple([
  v.literal('inspect'),
  inspectDownBodySchema,
]);

export type InspectDownMessage = v.Infer<typeof inspectDownMessageSchema>;

export type InspectDownBody = v.Infer<typeof inspectDownBodySchema>;
