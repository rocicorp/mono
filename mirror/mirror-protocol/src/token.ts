import * as v from 'shared/src/valita.js';
import {baseResponseFields} from './base.js';
import {createCaller} from './call.js';

export const createTokenRequestSchema = v.object({
  key: v.string(),
});

export const createTokenResponseSchema = v.object({
  ...baseResponseFields,
  token: v.string(),
});

export type CreateTokenRequest = v.Infer<typeof createTokenRequestSchema>;
export type CreateTokenResponse = v.Infer<typeof createTokenResponseSchema>;

export const createToken = createCaller(
  'token-create',
  createTokenRequestSchema,
  createTokenResponseSchema,
);
