import * as v from '../../shared/src/valita.ts';

const inspectUpBase = v.object({
  id: v.string(),
});

const inspectQueriesUpBodySchema = inspectUpBase.extend({
  op: v.literal('queries'),
  clientID: v.string().optional(),
});

export type InspectQueriesUpBody = v.Infer<typeof inspectQueriesUpBodySchema>;

const inspectBasicUpSchema = inspectUpBase.extend({
  op: v.literalUnion('metrics', 'version'),
});

export type InspectMetricsUpBody = {
  op: 'metrics';
  id: string;
};

export type InspectVersionUpBody = {
  op: 'version';
  id: string;
};

const inspectUpBodySchema = v.union(
  inspectQueriesUpBodySchema,
  inspectBasicUpSchema,
);

export const inspectUpMessageSchema = v.tuple([
  v.literal('inspect'),
  inspectUpBodySchema,
]);

export type InspectUpMessage = v.Infer<typeof inspectUpMessageSchema>;

export type InspectUpBody = v.Infer<typeof inspectUpBodySchema>;
