import {jsonSchema} from '../../shared/src/json-schema.ts';
import {tdigestSchema} from '../../shared/src/tdigest-schema.ts';
import * as v from '../../shared/src/valita.ts';
import {astSchema} from './ast.ts';

const serverMetricsSchema = v.object({
  'query-materialization-server': tdigestSchema,
  'query-update-server': tdigestSchema,
});

export type ServerMetrics = v.Infer<typeof serverMetricsSchema>;

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
  metrics: serverMetricsSchema.nullable().optional(),
});

export type InspectQueryRow = v.Infer<typeof inspectQueryRowSchema>;

const inspectBaseDownSchema = v.object({
  id: v.string(),
});

export const inspectQueriesDownSchema = inspectBaseDownSchema.extend({
  op: v.literal('queries'),
  value: v.array(inspectQueryRowSchema),
});

export type InspectQueriesDown = v.Infer<typeof inspectQueriesDownSchema>;

export const inspectMetricsDownSchema = inspectBaseDownSchema.extend({
  op: v.literal('metrics'),
  value: serverMetricsSchema,
});

export type InspectMetricsDown = v.Infer<typeof inspectMetricsDownSchema>;

export const inspectVersionDownSchema = inspectBaseDownSchema.extend({
  op: v.literal('version'),
  value: v.string(),
});

export const inspectDownBodySchema = v.union(
  inspectQueriesDownSchema,
  inspectMetricsDownSchema,
  inspectVersionDownSchema,
);

export const inspectDownMessageSchema = v.tuple([
  v.literal('inspect'),
  inspectDownBodySchema,
]);

export type InspectDownMessage = v.Infer<typeof inspectDownMessageSchema>;

export type InspectDownBody = v.Infer<typeof inspectDownBodySchema>;
