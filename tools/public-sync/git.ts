// Helpers shared by sync.ts and port-pr.ts.

import {execFileSync} from 'node:child_process';
import {rmSync, writeFileSync} from 'node:fs';
import {join} from 'node:path';

export const FILTER_PATH = 'tools/public-sync/public-filter';

// raw: return the output untouched instead of trimming trailing whitespace,
// for output that must round-trip exactly (a patch fed to `git apply`).
type Options = {
  env?: NodeJS.ProcessEnv | undefined;
  input?: string | undefined;
  raw?: boolean | undefined;
};

export function git(...a: [...string[], Options] | string[]): string {
  const last = a.at(-1);
  const {raw, ...opts} = typeof last === 'object' ? (a.pop() as Options) : {};
  const out = execFileSync('git', a as string[], {
    encoding: 'utf8',
    maxBuffer: 256 * 1024 * 1024,
    stdio: ['pipe', 'pipe', 'inherit'],
    ...opts,
  });
  return raw ? out : out.trimEnd();
}

export function lines(s: string): string[] {
  return s.split('\n').filter(l => l !== '');
}

/**
 * Loads `treeish` into a scratch index in `tmp` and returns the index env
 * plus the NUL-separated paths in it that `filterAt:public-filter` keeps
 * private. Uses git's own .gitignore matcher, so the filter behaves exactly
 * like a .gitignore file would.
 */
export function privatePaths(
  treeish: string,
  filterAt: string,
  tmp: string,
): {env: NodeJS.ProcessEnv; paths: string} {
  const index = join(tmp, 'index');
  const filter = join(tmp, 'filter');
  const env = {...process.env, GIT_INDEX_FILE: index};
  rmSync(index, {force: true});
  writeFileSync(filter, git('show', `${filterAt}:${FILTER_PATH}`));
  git('read-tree', treeish, {env});
  const paths = git(
    'ls-files',
    '-z',
    '--cached',
    '--ignored',
    `--exclude-from=${filter}`,
    {env},
  );
  return {env, paths};
}
