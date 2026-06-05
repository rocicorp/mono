import {jsonSchema} from '../../shared/src/json-schema.ts';
import * as v from '../../shared/src/valita.ts';
import {astSchema} from './ast.ts';

const inspectUpBase = v.object({
  id: v.string(),
});

const inspectQueriesUpBodySchema = inspectUpBase.extend({
  op: v.literal('queries'),
  clientID: v.string().optional(),
});

export type InspectQueriesUpBody = v.Infer<typeof inspectQueriesUpBodySchema>;

const inspectMetricsUpSchema = inspectUpBase.extend({
  op: v.literal('metrics'),
});

export type InspectMetricsUpBody = v.Infer<typeof inspectMetricsUpSchema>;

const inspectVersionUpSchema = inspectUpBase.extend({
  op: v.literal('version'),
});

export type InspectVersionUpBody = v.Infer<typeof inspectVersionUpSchema>;

export const inspectAuthenticateUpSchema = inspectUpBase.extend({
  op: v.literal('authenticate'),
  value: v.string(),
});

export type InspectAuthenticateUpBody = v.Infer<
  typeof inspectAuthenticateUpSchema
>;

const analyzeQueryOptionsSchema = v.object({
  vendedRows: v.boolean().optional(),
  syncedRows: v.boolean().optional(),
  joinPlans: v.boolean().optional(),
});

export type AnalyzeQueryOptions = v.Infer<typeof analyzeQueryOptionsSchema>;

export const inspectAnalyzeQueryUpSchema = inspectUpBase.extend({
  op: v.literal('analyze-query'),
  /** @deprecated Use {@linkcode ast} instead */
  value: astSchema.optional(),
  options: analyzeQueryOptionsSchema.optional(),
  ast: astSchema.optional(),
  name: v.string().optional(),
  args: v.readonlyArray(jsonSchema).optional(),
});

export type InspectAnalyzeQueryUpBody = v.Infer<
  typeof inspectAnalyzeQueryUpSchema
>;

/**
 * A single join field set (on one side of one relationship hop) that needs to
 * be backed by an index. Produced on the client (which has the schema's
 * relationships and the client->server name mapping) and sent to the server,
 * which checks each one against the replica's actual indexes.
 */
export const indexRequirementSchema = v.object({
  /** The table the relationship is declared on (client name). */
  ownerTable: v.string(),
  /** The relationship name. */
  relationship: v.string(),
  /** 1-based hop index. Junction (many-to-many) relationships have 2 hops. */
  hop: v.number(),
  /** Total number of hops (1 for direct, 2 for junction). */
  hopCount: v.number(),
  /** Which side of the join this field set is on. */
  side: v.literalUnion('source', 'dest'),
  cardinality: v.literalUnion('one', 'many'),
  /** The table being looked up / indexed (client name). */
  clientTable: v.string(),
  /** The join fields (client names). */
  clientColumns: v.array(v.string()),
  /** The table being looked up / indexed (server name). */
  serverTable: v.string(),
  /** The join fields (server names) — i.e. the columns to index. */
  serverColumns: v.array(v.string()),
});

export type IndexRequirement = v.Infer<typeof indexRequirementSchema>;

export const inspectCheckIndexesUpSchema = inspectUpBase.extend({
  op: v.literal('check-indexes'),
  requirements: v.array(indexRequirementSchema),
});

export type InspectCheckIndexesUpBody = v.Infer<
  typeof inspectCheckIndexesUpSchema
>;

const inspectUpBodySchema = v.union(
  inspectQueriesUpBodySchema,
  inspectMetricsUpSchema,
  inspectVersionUpSchema,
  inspectAuthenticateUpSchema,
  inspectAnalyzeQueryUpSchema,
  inspectCheckIndexesUpSchema,
);

export const inspectUpMessageSchema = v.tuple([
  v.literal('inspect'),
  inspectUpBodySchema,
]);

export type InspectUpMessage = v.Infer<typeof inspectUpMessageSchema>;

export type InspectUpBody = v.Infer<typeof inspectUpBodySchema>;
