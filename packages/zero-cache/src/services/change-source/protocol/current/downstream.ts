/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import * as v from '../../../../../../shared/src/valita.ts';
import {resetRequiredSchema} from './control.ts';
import {
  beginSchema,
  commitSchema,
  dataChangeSchema,
  rollbackSchema,
} from './data.ts';
import {statusMessageSchema} from './status.ts';

const begin = v.tuple([
  v.literal('begin'),
  beginSchema,
  v.object({commitWatermark: v.string()}),
]);
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
  statusMessageSchema,
);

export type ChangeStreamMessage = v.Infer<typeof changeStreamMessageSchema>;
