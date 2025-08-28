import * as v from '../../shared/src/valita.ts';

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

const inspectUpBodySchema = v.union(
  inspectQueriesUpBodySchema,
  inspectMetricsUpSchema,
  inspectVersionUpSchema,
  inspectAuthenticateUpSchema,
);

export const inspectUpMessageSchema = v.tuple([
  v.literal('inspect'),
  inspectUpBodySchema,
]);

export type InspectUpMessage = v.Infer<typeof inspectUpMessageSchema>;

export type InspectUpBody = v.Infer<typeof inspectUpBodySchema>;
