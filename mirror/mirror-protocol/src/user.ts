import * as v from 'shared/src/valita.js';
import {baseRequestFields, baseResponseFields} from './base.js';
import {createCaller} from './call.js';

export const ensureUserRequestSchema = v.object(baseRequestFields);
export type EnsureUserRequest = v.Infer<typeof ensureUserRequestSchema>;

export const ensureUserResponseSchema = v.object({
  ...baseResponseFields,
});
export type EnsureUserResponse = v.Infer<typeof ensureUserResponseSchema>;

export const ensureUser = createCaller(
  'user-ensure',
  ensureUserRequestSchema,
  ensureUserResponseSchema,
);
