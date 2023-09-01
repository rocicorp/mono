import * as fs from 'node:fs/promises';
import {sha256OfString} from 'shared/src/sha256.js';

async function fileChanged(absPath: string, hashes: Map<string, string>) {
  const content = await fs.readFile(absPath, 'utf-8');
  const hash = await sha256OfString(content);
  const oldHash = hashes.get(absPath);
  if (oldHash === hash) {
    return false;
  }
  hashes.set(absPath, hash);
  return true;
}

async function watchFile(
  absPath: string,
  signal: AbortSignal,
  hashes: Map<string, string>,
) {
  try {
    for await (const _ of fs.watch(absPath, {signal})) {
      if (await fileChanged(absPath, hashes)) {
        break;
      }
    }
  } catch (e) {
    if (e instanceof Error && e.name === 'AbortError') {
      return;
    }
    throw e;
  }
}

/**
 * Watches the files and resolves when any of them changes.
 * @param inputs
 * @param signal When this signal is
 * @param hashes Map of file path to hash of the file content. This is used to ignore changes that didn't change the content.
 */
export async function watchFiles(
  inputs: string[],
  signal: AbortSignal,
  hashes: Map<string, string>,
) {
  // We use a new AbortController for all the files. When the first watch is
  // triggered we remove all the listeners and this function resolves.
  const ac = new AbortController();
  const abort = () => ac.abort();
  signal.addEventListener('abort', abort);
  await Promise.race(inputs.map(input => watchFile(input, ac.signal, hashes)));
  signal.removeEventListener('abort', abort);
  abort();
}
