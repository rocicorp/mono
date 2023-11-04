import type {Env} from 'reflect-shared';

const VARS_PREFIX = 'REFLECT_VAR_';

export function extractVars(env: object): Env {
  const vars = Object.fromEntries(
    Object.entries(env)
      .filter(([key]) => key.startsWith(VARS_PREFIX))
      .map(([key, value]) => {
        if (typeof value !== 'string') {
          throw new Error(
            `Variable ${key} is not of type string (${typeof value})`,
          );
        }
        return [key.substring(VARS_PREFIX.length), value];
      }),
  );
  // Runtime enforcement of readonly-ness.
  return Object.freeze(vars);
}
