// Types are externally exported under stable versions. Code within
// zero-cache, on the contrary, should reference the files in
// current/* so that it is versioned with the latest version.
export * as v0 from './current/mod.js';
