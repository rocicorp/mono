import * as v from 'shared/src/valita.js';
import {baseAppRequestFields} from './app.js';
import {baseResponseFields} from './base.js';
import {createCaller} from './call.js';

export const listVarsRequestSchema = v.object({
  ...baseAppRequestFields,
  decrypted: v.boolean(),
});

export const envSchema = v.object({
  name: v.string().optional(),
  vars: v.record(v.string()),
});

export type Env = v.Infer<typeof envSchema>;

export const listVarsResponseSchema = v.object({
  ...baseResponseFields,
  decrypted: v.boolean(),
  envs: v.record(envSchema),
});

export type ListVarsRequest = v.Infer<typeof listVarsRequestSchema>;
export type ListVarsResponse = v.Infer<typeof listVarsResponseSchema>;

export const setVarsRequestSchema = v.object({
  ...baseAppRequestFields,
  // TODO: env: v.string().optional()
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
  // TODO: env: v.string().optional()
  vars: v.array(v.string()),
});

export const deleteVarsResponseSchema = v.object({
  ...baseResponseFields,
  deploymentPath: v.string().optional(),
});

export type DeleteVarsRequest = v.Infer<typeof deleteVarsRequestSchema>;
export type DeleteVarsResponse = v.Infer<typeof deleteVarsResponseSchema>;

export const listVars = createCaller(
  'vars-list',
  listVarsRequestSchema,
  listVarsResponseSchema,
);

export const setVars = createCaller(
  'vars-set',
  setVarsRequestSchema,
  setVarsResponseSchema,
);

export const deleteVars = createCaller(
  'vars-delete',
  deleteVarsRequestSchema,
  deleteVarsResponseSchema,
);
