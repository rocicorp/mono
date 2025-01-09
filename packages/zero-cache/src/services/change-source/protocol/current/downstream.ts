import * as v from '../../../../../../shared/src/valita.js';
import {resetRequiredSchema} from './control.js';
import {
  beginSchema,
  commitSchema,
  dataChangeSchema,
  rollbackSchema,
} from './data.js';

const begin = v.tuple([v.literal('begin'), beginSchema]);
const data = v.tuple([v.literal('data'), dataChangeSchema]);
const commit = v.tuple([
  v.literal('commit'),
  commitSchema,
  v.object({watermark: v.string()}),
]);
const rollback = v.tuple([v.literal('rollback'), rollbackSchema]);

export type Begin = v.Infer<typeof begin>;
export type Data = v.Infer<typeof data>;
export type Commit = v.Infer<typeof commit>;
export type Rollback = v.Infer<typeof rollback>;

export const dataPlaneMessageSchema = v.union(begin, data, commit, rollback);
export type DataPlaneMessage = v.Infer<typeof dataPlaneMessageSchema>;

export const controlPlaneMessageSchema = v.tuple([
  v.literal('control'),
  resetRequiredSchema, // TODO: Add statusRequestedSchema
]);
export type ControlPlaneMessage = v.Infer<typeof controlPlaneMessageSchema>;

/** Downstream messages consist of data plane and control plane messages. */
export const changeSourceDownstreamSchema = v.union(
  dataPlaneMessageSchema,
  controlPlaneMessageSchema,
);

export type ChangeSourceDownstream = v.Infer<
  typeof changeSourceDownstreamSchema
>;
