import * as v from '../../shared/src/valita.ts';

const inspectQueriesUpBodySchema = v.object({
  op: v.literal('queries'),
  id: v.string(),
  clientID: v.string().optional(),
});

export type InspectQueriesUpBody = v.Infer<typeof inspectQueriesUpBodySchema>;

const inspectMetricsUpSchema = v.object({
  op: v.literal('metrics'),
  id: v.string(),
});

export type InspectMetricsUpBody = v.Infer<typeof inspectMetricsUpSchema>;

const inspectUpBodySchema = v.union(
  inspectQueriesUpBodySchema,
  inspectMetricsUpSchema,
);

export const inspectUpMessageSchema = v.tuple([
  v.literal('inspect'),
  inspectUpBodySchema,
]);

export type InspectUpMessage = v.Infer<typeof inspectUpMessageSchema>;

export type InspectUpBody = v.Infer<typeof inspectUpBodySchema>;
