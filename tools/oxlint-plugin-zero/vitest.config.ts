import {defineConfig} from 'vitest/config';

// Standalone rather than merging the shared tool config: these are pure unit
// tests over plain-JS predicates, so none of the shared browser/database setup
// applies, and reaching across packages for it would mean declaring a
// dependency this plugin does not otherwise have.
export default defineConfig({
  test: {
    // The plugin entry is a root-level index.js — that path is what
    // oxlint.base.ts loads — so tests sit beside it rather than under src/.
    include: ['*.test.js'],
    // Matches the shared config's 10s. The smoke tests run the real linter out of
    // process, so vitest's 5s default leaves less margin than anything else here,
    // and a blocking spawn cannot be interrupted by a timeout anyway.
    testTimeout: 10_000,
  },
});
