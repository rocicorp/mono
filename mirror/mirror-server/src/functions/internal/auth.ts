import {defineSecretSafely} from '../app/secrets.js';

export const INTERNAL_FUNCTION_HEADER = 'X-Mirror-Internal-Function';
export const INTERNAL_FUNCTION_SECRET_NAME = 'INTERNAL_FUNCTION_SECRET';
export const INTERNAL_FUNCTION_SECRET = defineSecretSafely(
  INTERNAL_FUNCTION_SECRET_NAME,
);
