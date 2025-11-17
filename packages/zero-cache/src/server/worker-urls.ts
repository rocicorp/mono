// This module provides URLs for worker files.

function resolve(path: string): URL {
  const {url} = import.meta;
  if (url.endsWith('.js')) {
    // When compiled, change .ts to .js
    path = path.replace(/\.ts$/, '.js');
  }
  return new URL(path, url);
}

// These URLs are part of the build process. See ../../zero/tool/build.ts
// All these urls must be relative to this file and be located in the same directory.

export const MAIN_URL = resolve('./main.ts');
export const MUTATOR_URL = resolve('./mutator.ts');
export const CHANGE_STREAMER_URL = resolve('./change-streamer.ts');
export const REAPER_URL = resolve('./reaper.ts');
export const REPLICATOR_URL = resolve('./replicator.ts');
export const SYNCER_URL = resolve('./syncer.ts');
