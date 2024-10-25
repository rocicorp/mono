// The env value should be filled in by esbuild.

declare const process: {
  env: {
    ['REPLICACHE_VERSION']?: string;
  };
};

/**
 * The current version of Replicache.
 */
export const version: string = process.env.REPLICACHE_VERSION ?? '0.0.0';
