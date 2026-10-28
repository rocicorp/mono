import {jsonSchema} from '../../shared/src/json-schema.ts';
import * as v from '../../shared/src/valita.ts';
import {strictASTSchema} from './ast.ts';

const inspectUpBase = v.object({
  id: v.string(),
});

const inspectQueriesUpBodySchema = inspectUpBase.extend({
  op: v.literal('queries'),
  clientID: v.string().optional(),
});

const inspectMetricsUpSchema = inspectUpBase.extend({
  op: v.literal('metrics'),
});

const inspectVersionUpSchema = inspectUpBase.extend({
  op: v.literal('version'),
});

const inspectAuthenticateUpSchema = inspectUpBase.extend({
  op: v.literal('authenticate'),
  value: v.string(),
});

const analyzeQueryOptionsSchema = v.object({
  vendedRows: v.boolean().optional(),
  syncedRows: v.boolean().optional(),
  joinPlans: v.boolean().optional(),
});

export type AnalyzeQueryOptions = v.Infer<typeof analyzeQueryOptionsSchema>;

export const inspectAnalyzeQueryUpSchema = inspectUpBase.extend({
  op: v.literal('analyze-query'),
  /** @deprecated Use {@link ast} instead */
  value: strictASTSchema.optional(),
  options: analyzeQueryOptionsSchema.optional(),
  ast: strictASTSchema.optional(),
  name: v.string().optional(),
  args: v.readonlyArray(jsonSchema).optional(),
});

const unparsedInspectAnalyzeQueryUpSchema = inspectUpBase.extend({
  op: v.literal('analyze-query'),
  /** @deprecated Use {@link ast} instead */
  value: v.unknown().optional(),
  options: analyzeQueryOptionsSchema.optional(),
  ast: v.unknown().optional(),
  name: v.string().optional(),
  args: v.readonlyArray(jsonSchema).optional(),
});

const inspectUpBodySchema = v.union(
  inspectQueriesUpBodySchema,
  inspectMetricsUpSchema,
  inspectVersionUpSchema,
  inspectAuthenticateUpSchema,
  inspectAnalyzeQueryUpSchema,
);

export const inspectUpMessageSchema = v.tuple([
  v.literal('inspect'),
  inspectUpBodySchema,
]);

// zero-cache answers an op from a newer client with an error for that request
// instead of closing the connection.
export const unparsedInspectUpBodySchema = v.taggedObjectUnion(
  'op',
  [
    inspectQueriesUpBodySchema,
    inspectMetricsUpSchema,
    inspectVersionUpSchema,
    inspectAuthenticateUpSchema,
    unparsedInspectAnalyzeQueryUpSchema,
  ],
  {open: true},
);

export const unparsedInspectUpMessageSchema = v.tuple([
  v.literal('inspect'),
  unparsedInspectUpBodySchema,
]);

export type InspectUpBody = v.Infer<typeof inspectUpBodySchema>;

export type UnparsedInspectUpMessage = v.Infer<
  typeof unparsedInspectUpMessageSchema
>;

/** An inspect request with an op this build knows. */
export type UnparsedInspectUpBody = Exclude<
  v.Infer<typeof unparsedInspectUpBodySchema>,
  v.UnrecognizedObject<'op'>
>;
