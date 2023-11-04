export const SERVER_VARIABLE_PREFIX = 'REFLECT_VAR_';
export const ALLOWED_SERVER_VARIABLE_CHARS = /^[A-Za-z0-9_]+$/;
export const MAX_SERVER_VARIABLES = 50;

// Cloudflare's (undocumented) limit is 2712 bytes, above which
// it will return {code: 10100, message: "workers.api.error.binding_name_too_large"}.
// Firestore's limit on field paths is 1500 bytes.
// We set a safe limit of 1K.
const MAX_ENCODED_NAME_SIZE = 1024;

// Cloudflare's limit is 5120 bytes for the value only, above which
// it will return {code: 10054, message: "workers.api.error.text_binding_too_large"}.
const MAX_ENCODED_TOTAL_SIZE = 5 * 1024;

export function variableNameIsWithinSizeLimit(name: string): boolean {
  return Buffer.byteLength(name) <= MAX_ENCODED_NAME_SIZE;
}

export function variableIsWithinSizeLimit(
  name: string,
  value: string,
): boolean {
  return (
    Buffer.byteLength(name) + Buffer.byteLength(value) <= MAX_ENCODED_TOTAL_SIZE
  );
}
