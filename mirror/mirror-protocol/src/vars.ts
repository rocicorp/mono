import * as v from 'shared/src/valita.js';
import {baseAppRequestFields} from './app.js';
import {baseResponseFields} from './base.js';
import {createCall} from './call.js';

export const listVarsRequestSchema = v.object({
  ...baseAppRequestFields,
  decrypted: v.boolean(),
});

export const listVarsResponseSchema = v.object({
  ...baseResponseFields,
  decrypted: v.boolean(),
  vars: v.record(v.string()),
});

export type ListVarsRequest = v.Infer<typeof listVarsRequestSchema>;
export type ListVarsResponse = v.Infer<typeof listVarsResponseSchema>;

export const setVarsRequestSchema = v.object({
  ...baseAppRequestFields,
  vars: v.record(v.string()),
});

export const setVarsResponseSchema = v.object({
  ...baseResponseFields,
  deploymentPath: v.string().optional(),
});

export type SetVarsRequest = v.Infer<typeof setVarsRequestSchema>;
export type SetVarsResponse = v.Infer<typeof setVarsResponseSchema>;

export const deleteVarsRequestSchema = v.object({
  ...baseAppRequestFields,
  vars: v.array(v.string()),
});

export const deleteVarsResponseSchema = v.object({
  ...baseResponseFields,
  deploymentPath: v.string().optional(),
});

export type DeleteVarsRequest = v.Infer<typeof deleteVarsRequestSchema>;
export type DeleteVarsResponse = v.Infer<typeof deleteVarsResponseSchema>;

export const listVars = createCall(
  'vars-list',
  listVarsRequestSchema,
  listVarsResponseSchema,
);

export const setVars = createCall(
  'vars-set',
  setVarsRequestSchema,
  setVarsResponseSchema,
);

export const deleteVars = createCall(
  'vars-delete',
  deleteVarsRequestSchema,
  deleteVarsResponseSchema,
);
