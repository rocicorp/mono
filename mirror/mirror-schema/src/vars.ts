export const SERVER_VARIABLE_PREFIX = 'REFLECT_VAR_';
export const ALLOWED_SERVER_VARIABLE_CHARS = /^[A-Za-z0-9_]+$/;
export const MAX_SERVER_VARIABLES = 50;

const MAX_ENCODED_VARIABLE_SIZE = 5 * 1024;

export function variableIsWithinSizeLimit(
  name: string,
  value: string,
): boolean {
  return (
    Buffer.byteLength(name) + Buffer.byteLength(value) <=
    MAX_ENCODED_VARIABLE_SIZE
  );
}
