/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-call, @typescript-eslint/await-thenable, @typescript-eslint/no-misused-promises, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-floating-promises, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/require-await, @typescript-eslint/no-empty-object-type, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error */
// The env value should be filled in by esbuild.

declare const process: {
  env: {
    ['ZERO_VERSION']?: string;
  };
};

/**
 * The current version of Zero.
 */
export const version = process.env.ZERO_VERSION ?? '0.0.0';
