import * as v from '../../../../../../shared/src/valita.ts';
import {resetRequiredSchema} from './control.ts';
import {
  beginSchema,
  commitSchema,
  dataChangeSchema,
  rollbackSchema,
  schemaChangeSchema,
} from './data.ts';
import {downstreamStatusMessageSchema} from './status.ts';

const begin = v.tuple([
  v.literal('begin'),
  beginSchema,
  v.object({commitWatermark: v.string()}),
]);
const data = v.tuple([
  v.literal('data'),
  v.union(dataChangeSchema, schemaChangeSchema),
]);
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

export const changeStreamDataSchema = v.union(begin, data, commit, rollback);
export type ChangeStreamData = v.Infer<typeof changeStreamDataSchema>;

export const changeStreamControlSchema = v.tuple([
  v.literal('control'),
  resetRequiredSchema, // TODO: Add statusRequestedSchema
]);
export type ChangeStreamControl = v.Infer<typeof changeStreamControlSchema>;

/** Downstream messages consist of data plane and control plane messages. */
export const changeStreamMessageSchema = v.union(
  changeStreamDataSchema,
  changeStreamControlSchema,
  downstreamStatusMessageSchema,
);

export type ChangeStreamMessage = v.Infer<typeof changeStreamMessageSchema>;
