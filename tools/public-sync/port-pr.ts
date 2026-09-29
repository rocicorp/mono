#!/usr/bin/env node
// oxlint-disable no-console
// Ports an outside contribution from a rocicorp/mono PR into mono-internal.
//
// PRs are never merged on mono itself: sync.ts refuses to publish once mono
// has commits it didn't write. Instead a Roci engineer runs this from a
// mono-internal checkout. It applies the PR's diff (as a squash, so merges of
// main into the PR branch don't matter) onto a new branch off mono-internal's
// main, commits it as the contributor with any other PR authors as
// Co-authored-by, and opens a mono-internal PR.
//
// mono-internal squash-merges with the PR body as the commit message, so the
// mono-internal PR's body carries a Co-authored-by line for every PR author
// and a Ported-From line (see internalBody). sync.ts turns those into the
// contributor's credit and a `Closes` line on mono.
//
// Usage (from a mono-internal checkout):
//   node tools/public-sync/port-pr.ts <pr-number> [--no-push]
//     [--repo rocicorp/mono] [--internal-repo rocicorp/mono-internal]
//     [--onto origin/main] [--remote <git url of --repo>]
//
// Files the filter keeps private need care, since the contributor never saw
// mono-internal's versions of them:
// - New in the PR and absent from mono-internal: applied, with a warning that
//   they won't show up on mono.
// - New in the PR but already in mono-internal (say, a zero-cache test file
//   named like an existing private one): applying would clobber or conflict
//   with the private file, so it is left out of the patch and the
//   contributor's version is saved beside it as mono-<N>.<name>. The
//   internal PR lists these so they get folded in by hand before merging.
// - Changed by the PR and present in its base (only for PRs based on mono's
//   history from before the rewrite): merged by `git apply --3way` like any
//   other change.

import {execFileSync} from 'node:child_process';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import {tmpdir} from 'node:os';
import {dirname, join} from 'node:path';
import {parseArgs} from 'node:util';
import {git, lines, privatePaths} from './git.ts';

const {values: args, positionals} = parseArgs({
  allowPositionals: true,
  options: {
    'repo': {type: 'string', default: 'rocicorp/mono'},
    'internal-repo': {type: 'string', default: 'rocicorp/mono-internal'},
    'onto': {type: 'string', default: 'origin/main'},
    'no-push': {type: 'boolean', default: false},
    'remote': {type: 'string'},
  },
});
const number = positionals[0];
if (!number || !/^\d+$/.test(number)) {
  throw new Error('Usage: port-pr.ts <pr-number> [--no-push]');
}

type PR = {
  title: string;
  url: string;
  state: string;
  merged: boolean;
  author: string;
  base: string;
};
const pr = JSON.parse(
  gh(
    'api',
    `repos/${args.repo}/pulls/${number}`,
    '--jq',
    '{title, url: .html_url, state, merged, author: .user.login, base: .base.ref}',
  ),
) as PR;
if (pr.merged) {
  throw new Error(`${pr.url} is already merged on ${args.repo}.`);
}

const tmp = mkdtempSync(join(tmpdir(), 'port-pr-'));
try {
  main();
} finally {
  rmSync(tmp, {recursive: true, force: true});
}

function main() {
  // Everything staged ends up in the port commit, so start from a clean tree.
  if (git('status', '--porcelain', '--untracked-files=no') !== '') {
    throw new Error('Commit or stash your changes before porting.');
  }

  // Base the port, and the private-file checks below, on the current main
  // rather than whatever was last fetched.
  const slash = args.onto.indexOf('/');
  const remote = slash === -1 ? '' : args.onto.slice(0, slash);
  const isRemote = lines(git('remote')).includes(remote);
  const ontoBranch = isRemote ? args.onto.slice(slash + 1) : args.onto;
  // A PR against a release branch must not end up on main.
  if (pr.base !== ontoBranch) {
    throw new Error(
      `${pr.url} targets ${pr.base}, not ${ontoBranch}. Pass ` +
        `--onto origin/${pr.base} to port it onto that branch.`,
    );
  }
  if (isRemote) {
    git('fetch', '--no-tags', remote, ontoBranch);
  }

  const url = args.remote ?? `https://github.com/${args.repo}.git`;
  const headRef = `refs/port-pr/${number}/head`;
  const baseRef = `refs/port-pr/${number}/base`;
  git(
    'fetch',
    '--no-tags',
    url,
    `+refs/pull/${number}/head:${headRef}`,
    `+refs/heads/${pr.base}:${baseRef}`,
  );
  const head = git('rev-parse', headRef);
  // mono's commits are filtered copies of mono-internal's, so the PR's own
  // changes are exactly its diff against the point it branched from mono.
  const base = git('merge-base', baseRef, head);

  // -z: raw paths, not C-quoted, so they compare equal to privatePaths'.
  const changed = git('diff', '--name-only', '-z', base, head)
    .split('\0')
    .filter(p => p !== '');
  if (changed.length === 0) {
    throw new Error(`${pr.url} has no changes against ${pr.base}.`);
  }
  const prAuthors = authors(base, head);
  if (prAuthors.length === 0) {
    throw new Error(
      `${pr.url} has no non-merge commits to take an author from.`,
    );
  }
  const priv = privateChanges(head, changed);
  const collisions = priv.filter(p => !exists(base, p) && exists(args.onto, p));
  const added = priv.filter(p => !collisions.includes(p));
  if (added.length) {
    console.warn(
      `These files are private and won't show up on ${args.repo}:\n` +
        added.map(p => `  ${p}`).join('\n'),
    );
  }
  // Only a PR based on mono's history from before the rewrite can delete or
  // rename away a file that is private now. The patch removes it from
  // mono-internal too, which is probably what the contributor meant, but
  // it's a file they can no longer see, so call it out.
  const removed = privateChanges(base, changed).filter(p => !exists(head, p));
  if (removed.length) {
    console.warn(
      `The PR deletes or renames these private files, so the port removes ` +
        `them from mono-internal too; check that's intended:\n` +
        removed.map(p => `  ${p}`).join('\n'),
    );
  }

  // A rerun after a failed attempt finds the branch already there. Start it
  // over unless it holds commits of its own (a finished port).
  const branch = `port/mono-${number}`;
  if (
    git('branch', '--list', branch) !== '' &&
    git('rev-list', `${args.onto}..${branch}`) !== ''
  ) {
    throw new Error(
      `${branch} already has commits. Delete it (git branch -D ${branch}) ` +
        'to port again.',
    );
  }
  git('switch', '-C', branch, args.onto);

  // --3way falls back to a merge where mono-internal has moved on since the
  // PR's base; the blobs it needs came with the fetch.
  const patch = git(
    'diff',
    '--binary',
    '--full-index',
    base,
    head,
    '--',
    '.',
    ...collisions.map(p => `:(exclude,literal)${p}`),
    {raw: true},
  );
  const aside = collisions.map(p => ({path: p, saved: saveAside(head, p)}));
  if (aside.length) {
    // The copies must stay private too, or the sync would publish them.
    const {paths} = privatePaths(git('write-tree'), args.onto, tmp);
    const priv = new Set(paths.split('\0'));
    const leaked = aside.filter(a => !priv.has(a.saved));
    if (leaked.length) {
      throw new Error(
        `public-filter would publish these saved copies; rename them by ` +
          `hand:\n${leaked.map(a => `  ${a.saved}`).join('\n')}`,
      );
    }
    console.warn(
      `These files already exist privately in mono-internal. The ` +
        `contributor's versions were saved beside them; fold them in by ` +
        `hand before merging:\n` +
        aside.map(a => `  ${a.saved} -> ${a.path}`).join('\n'),
    );
  }

  const [author, ...coAuthors] = prAuthors;
  const message = [
    pr.title,
    '',
    `Ported from ${pr.url} by @${pr.author}.`,
    ...(coAuthors.length
      ? ['', ...coAuthors.map(a => `Co-authored-by: ${a}`)]
      : []),
  ].join('\n');
  const body = internalBody(prAuthors, aside);

  try {
    if (patch) {
      git('apply', '--3way', '--index', {input: patch});
    }
  } catch {
    // Keep the commit message and PR body: the body's Ported-From and
    // Co-authored-by lines are what let the sync close the public PR and
    // credit the contributor.
    const messageFile = git('rev-parse', '--git-path', `${branch}.message`);
    const bodyFile = git('rev-parse', '--git-path', `${branch}.body`);
    mkdirSync(dirname(messageFile), {recursive: true});
    writeFileSync(messageFile, message + '\n');
    writeFileSync(bodyFile, body + '\n');
    console.error(
      `\nThe diff did not apply cleanly. Resolve the conflicts on ${branch}, then:\n` +
        `  git commit --author ${shell(author)} -F ${shell(messageFile)}\n` +
        `  git push -u origin ${branch}\n` +
        `  gh pr create --repo ${args['internal-repo']} --head ${branch} ` +
        `--title ${shell(pr.title)} --body-file ${shell(bodyFile)}`,
    );
    process.exit(1);
  }

  git('commit', '--author', author, '-F', '-', {input: message + '\n'});
  console.log(`Committed ${pr.title} as ${author} on ${branch}.`);

  if (args['no-push']) {
    return;
  }
  git('push', '-u', 'origin', branch);
  const internalPR = gh(
    'pr',
    'create',
    '--repo',
    args['internal-repo'],
    '--head',
    branch,
    '--title',
    pr.title,
    '--body',
    body,
  );
  console.log(internalPR);
}

/**
 * The squash merge uses the PR body as the commit body, so the lines sync.ts
 * looks for go here: `Ported-From`, which becomes `Closes <repo>#<number>` on
 * mono and closes the public PR once the change is published, and a
 * Co-authored-by for every contributor.
 */
function internalBody(
  prAuthors: string[],
  aside: {path: string; saved: string}[],
): string {
  return [
    `Ported from ${pr.url} by @${pr.author}.`,
    ...(aside.length
      ? [
          "Before merging, fold the contributor's versions of these private " +
            'files into the originals and delete the copies:\n\n' +
            aside.map(a => `- [ ] \`${a.saved}\` → \`${a.path}\``).join('\n'),
        ]
      : []),
    [
      `Ported-From: ${args.repo}#${number}`,
      ...prAuthors.map(a => `Co-authored-by: ${a}`),
    ].join('\n'),
  ].join('\n\n');
}

/**
 * Everyone who authored a commit in the PR, first committer first. The first
 * becomes the ported commit's author, the rest Co-authored-by.
 */
function authors(base: string, head: string): string[] {
  return [
    ...new Set(
      lines(
        git(
          'log',
          '--reverse',
          '--no-merges',
          '--format=%an <%ae>',
          `${base}..${head}`,
        ),
      ),
    ),
  ];
}

/** The PR's changed files that mono-internal's filter keeps private. */
function privateChanges(head: string, changed: string[]): string[] {
  const {paths} = privatePaths(head, args.onto, tmp);
  const priv = new Set(paths.split('\0'));
  return changed.filter(p => priv.has(p));
}

function exists(rev: string, path: string): boolean {
  return (
    git('--literal-pathspecs', 'ls-tree', '--name-only', rev, '--', path) !== ''
  );
}

/**
 * Writes the PR's version of `path` to a sibling that still matches the same
 * globs (`cvr.test.ts` -> `mono-6694.cvr.test.ts`), straight from the object
 * store so binary files survive, and stages it.
 */
function saveAside(head: string, path: string): string {
  const [mode, , sha] = git('--literal-pathspecs', 'ls-tree', head, '--', path)
    .split('\t')[0]
    .split(' ');
  // A prefix, not an infix: the private patterns match a file's end
  // (*.test.ts, *-gen.ts, *-tests.ts) or a substring (*bench*), and a prefix
  // keeps every one of those matching.
  const slash = path.lastIndexOf('/') + 1;
  const saved = `${path.slice(0, slash)}mono-${number}.${path.slice(slash)}`;
  // checkout-index -f would overwrite an untracked file of the same name.
  if (existsSync(join(git('rev-parse', '--show-toplevel'), saved))) {
    throw new Error(`${saved} already exists; move it out of the way first.`);
  }
  git('update-index', '--add', '--cacheinfo', `${mode},${sha},${saved}`);
  git('checkout-index', '-f', '--', saved);
  return saved;
}

/** Quotes `s` for a POSIX shell command line printed for the user. */
function shell(s: string): string {
  return `'${s.replaceAll("'", `'\\''`)}'`;
}

function gh(...a: string[]): string {
  return execFileSync('gh', a, {encoding: 'utf8'}).trimEnd();
}
