/**
 * Ambient typing for the `process.env` flags that builds inline via defines
 * (see `makeDefine` in shared/src/build.ts and packages/zero/tool/build.ts).
 *
 * Import this module for its side effect (the global declaration). Keep the
 * flags as bare `process.env.X` expressions at every use site — never hoist
 * them into a shared const or re-export values from here, because bundlers
 * only constant-fold and dead-code-eliminate the inlined expression itself.
 *
 * Declared so that it merges cleanly with @types/node in programs that load
 * it (e.g. via *.test.node.ts files) and stands alone in browser-only
 * programs: interfaces merge, and the `var` redeclaration is allowed because
 * the type is identical.
 */
declare global {
  namespace NodeJS {
    interface ProcessEnv {
      ['DISABLE_MUTATION_RECOVERY']?: boolean | undefined;
      ['DISABLE_REPLICACHE_INDEXES']?: boolean | undefined;
    }
    interface Process {
      env: ProcessEnv;
    }
  }

  var process: NodeJS.Process;
}

export {};
