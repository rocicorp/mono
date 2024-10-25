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
