#!/usr/bin/env node
// oxlint-disable no-console
// Publishes mono-internal's main branch to rocicorp/mono. Runs on
// mono-internal CI after every push to main.
//
// mono-internal starts life as a full-history duplicate of mono (see
// gh-duplicate-repo.ts), so both repos share every commit up to the split.
// From then on, for every first-parent commit on mono-internal/main this
// builds a tree with only the files `public-filter` allows and commits it on
// top of mono's main. The mono commit keeps the title and author and adds a
// trailer naming the source commit. It has no body and no internal parents,
// so nothing written after the split, private files or internal history,
// becomes reachable from mono.
//
// What mono already had before the split stays reachable through its history,
// private files included, until mono's history is rewritten (step 6 of the
// rollout, with git filter-repo --preserve-commit-hashes). That rewrite comes
// after the sync is running, so the sync can't require it up front.
//
// There is no state besides mono itself: the newest trailer on mono's main
// says how far the sync got, so reruns and overlapping runs converge. The push
// is a plain fast-forward, so a lost race fails instead of overwriting.
//
// Usage (from a mono-internal checkout with full history):
//   GH_TOKEN=... node tools/public-sync/sync.ts [--dry-run]
//     [--public https://github.com/rocicorp/mono.git] [--source HEAD]
//     [--branch main] [--sign]
//
// --sign SSH-signs each commit with the bot's key (git config gpg.format=ssh
// and user.signingkey). git commit-tree ignores commit.gpgSign, so signing
// has to be asked for explicitly.

import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {parseArgs} from 'node:util';
import {FILTER_PATH, git, lines, privatePaths} from './git.ts';

const TRAILER = 'Mono-Internal-Commit';
const PUBLIC_REF = 'refs/public-sync/head';
const PR_SUFFIX = /\s*\(#\d+\)$/;
// A bare `#123`; `owner/repo#123` is left alone.
const BARE_REF = /(?<![\w/])#(\d+)\b/g;
const PORTED_FROM = /^ported-from:\s*([\w.-]+\/[\w.-]+#\d+)$/i;
const CO_AUTHOR = /^co-authored-by:\s*(.+)$/i;

// Co-authors that are coding agents or bots. Crediting them on mono adds
// nothing, so they are dropped. Matched against `Name <email>`.
const AGENT_CO_AUTHORS = [
  /<noreply@anthropic\.com>$/i, // Claude
  /<[^>]*copilot[^>]*>$/i, // GitHub Copilot, in its several spellings
  /<cursoragent@cursor\.com>$/i, // Cursor
  /@openai\.com>$/i, // Codex
  /\[bot\]\s*</i, // Any GitHub App bot
];

const {values: args} = parseArgs({
  options: {
    'public': {type: 'string', default: 'https://github.com/rocicorp/mono.git'},
    'source': {type: 'string', default: 'HEAD'},
    'branch': {type: 'string', default: 'main'},
    'dry-run': {type: 'boolean', default: false},
    'sign': {type: 'boolean', default: false},
  },
});

// Authenticates fetch and push to github.com without putting the token in
// the remote URL (same header actions/checkout uses). extraheader is
// multi-valued, so first clear any header actions/checkout persisted for
// mono-internal's own token; otherwise git sends both.
const auth = process.env.GH_TOKEN
  ? [
      '-c',
      'http.https://github.com/.extraheader=',
      '-c',
      `http.https://github.com/.extraheader=AUTHORIZATION: basic ${Buffer.from(
        `x-access-token:${process.env.GH_TOKEN}`,
      ).toString('base64')}`,
    ]
  : [];

// The first source commit that has public-filter; see filterFor.
let firstFilterCommit: string | undefined;

const tmp = mkdtempSync(join(tmpdir(), 'public-sync-'));
try {
  main();
} finally {
  rmSync(tmp, {recursive: true, force: true});
}

function main() {
  git(
    ...auth,
    'fetch',
    '--no-tags',
    args.public,
    `+${args.branch}:${PUBLIC_REF}`,
  );
  const publicHead = git('rev-parse', PUBLIC_REF);
  const sourceHead = git('rev-parse', args.source);

  let parent = publicHead;
  const last = findLastSynced(PUBLIC_REF);
  let from = last?.source;

  if (last === undefined || from === undefined) {
    // First run after the duplication: nothing has been synced yet, and mono's
    // head is still a commit in mono-internal's own history. Start there, and
    // drop the private files in one dedicated commit rather than folding that
    // deletion into whichever feature commit happens to come next.
    if (!isAncestor(publicHead, sourceHead)) {
      throw new Error(
        `${args.branch} has no ${TRAILER} trailer and ${publicHead} is not ` +
          `in ${args.source}'s history, so there is nowhere to start from.`,
      );
    }
    from = publicHead;
    parent = commitTree(
      filteredTree(from),
      parent,
      `chore: stop publishing private sources\n\n${TRAILER}: ${from}\n`,
    );
    console.log(`split at ${from.slice(0, 10)} -> ${parent.slice(0, 10)}`);
  } else {
    // The split commit's trailer names a pre-split mono commit. git
    // filter-repo rewrites commit hashes it finds in messages unless run with
    // --preserve-commit-hashes, which would leave a SHA mono-internal lacks.
    if (!isAncestor(from, sourceHead)) {
      throw new Error(
        `${args.branch}'s newest ${TRAILER} trailer names ${from}, which is ` +
          `not in ${args.source}'s history. If mono's history was rewritten, ` +
          'redo it with git filter-repo --preserve-commit-hashes.',
      );
    }
    // Every commit on mono's main after the split is one this script wrote,
    // so its head must be the newest one carrying the trailer. Anything
    // else -- even an empty commit, or a change pushed and then reverted --
    // was pushed to mono directly and would otherwise be built on.
    if (last.commit !== publicHead) {
      throw new Error(
        `${args.branch} has commits after the last sync (${last.commit}); ` +
          `its head is ${publicHead}. Port any direct changes to ` +
          'mono-internal and reset mono to the last sync commit first.',
      );
    }
    // Same for a head that carries the trailer but whose tree differs from
    // what we'd publish, e.g. an amended sync commit. Refuse rather than
    // silently revert it; the change has to land in mono-internal first.
    const expected = filteredTree(from);
    const actual = treeOf(publicHead);
    if (expected !== actual) {
      throw new Error(
        `${args.branch} has diverged from mono-internal@${from}. ` +
          `Port the direct changes to mono-internal first:\n` +
          `  git diff ${expected} ${actual}`,
      );
    }
  }

  let parentTree = treeOf(parent);
  const todo = lines(
    git('rev-list', '--reverse', '--first-parent', `${from}..${sourceHead}`),
  );
  for (const sha of todo) {
    const tree = filteredTree(sha);
    const body = lines(git('show', '-s', '--format=%b', sha));
    // A port that only touched private files still gets an (empty) commit, so
    // its Closes line closes the public PR.
    if (tree === parentTree && matchLines(body, PORTED_FROM).length === 0) {
      // Commit only touched private files. Skipping it keeps its title
      // private too; the next public commit's trailer moves past it.
      console.log(`skip ${sha.slice(0, 10)} (no public changes)`);
      continue;
    }
    parent = commitTree(tree, parent, publicMessage(sha, body), authorOf(sha));
    parentTree = tree;
    console.log(`${sha.slice(0, 10)} -> ${parent.slice(0, 10)}`);
  }

  if (parent === publicHead) {
    console.log('Nothing to publish.');
    return;
  }
  if (args['dry-run']) {
    console.log(`dry run: would push ${parent} to ${args.branch}`);
    return;
  }
  // Not --force: if another run got there first this fails, and the next
  // run picks up from its trailer.
  git(...auth, 'push', args.public, `${parent}:refs/heads/${args.branch}`);
}

/**
 * The newest commit on `ref` carrying our trailer, or undefined before the
 * first sync.
 */
function findLastSynced(
  ref: string,
): {commit: string; source: string} | undefined {
  // --grep + -1 stops at the newest synced commit instead of formatting all of
  // mono's history on every run.
  const out = git(
    'log',
    '-1',
    '--first-parent',
    `--grep=^${TRAILER}: `,
    `--format=%H%x00%(trailers:key=${TRAILER},valueonly,separator=)`,
    ref,
  );
  const [commit, source] = out.split('\0');
  return commit && source ? {commit, source} : undefined;
}

/**
 * Tree of `sha` minus everything the filter excludes. Uses the filter as it
 * was at `sha`, so filter changes ship atomically with the code they cover.
 * Works entirely on objects; never touches the working tree.
 */
function filteredTree(sha: string): string {
  const {env, paths} = privatePaths(sha, filterFor(sha), tmp);
  if (paths) {
    git('update-index', '-z', '--force-remove', '--stdin', {env, input: paths});
  }
  return git('write-tree', {env});
}

function commitTree(
  tree: string,
  parent: string,
  message: string,
  author: NodeJS.ProcessEnv = {},
): string {
  // The committer is the CI bot (user.name/user.email from git config).
  const sign = args.sign ? ['-S'] : [];
  return git('commit-tree', ...sign, tree, '-p', parent, {
    input: message,
    env: {...process.env, ...author},
  });
}

function authorOf(sha: string): NodeJS.ProcessEnv {
  const [name, email, date] = git(
    'show',
    '-s',
    '--format=%an%x00%ae%x00%aI',
    sha,
  ).split('\0');
  return {
    GIT_AUTHOR_NAME: name,
    GIT_AUTHOR_EMAIL: email,
    GIT_AUTHOR_DATE: date,
  };
}

/**
 * The title, a `Closes` line for a ported outside PR, and the source's
 * Co-authored-by lines for people. The rest of the body can reference private
 * files, PRs and discussions, so it stays behind.
 *
 * port-pr.ts writes `Ported-From: rocicorp/mono#123` and a Co-authored-by for
 * each contributor into the mono-internal PR's body, which the squash merge
 * uses as the commit body. Only here, on mono, does that become a closing
 * keyword: in mono-internal it would close the public PR before the change
 * had been published.
 *
 * Both are matched on any line of the body, not only in the final trailer
 * paragraph, since GitHub's squash can put its own paragraph after them.
 */
function publicMessage(sha: string, body: string[]): string {
  const subject = git('show', '-s', '--format=%s', sha);
  const closes = matchLines(body, PORTED_FROM).map(ref => `Closes ${ref}`);
  const people = matchLines(body, CO_AUTHOR).filter(
    c => !AGENT_CO_AUTHORS.some(r => r.test(c)),
  );
  const trailers = [
    ...Array.from(new Set(people), c => `Co-authored-by: ${c}`),
    `${TRAILER}: ${sha}`,
  ];
  return (
    [
      publicSubject(subject),
      ...(closes.length ? [[...new Set(closes)].join('\n')] : []),
      trailers.join('\n'),
    ].join('\n\n') + '\n'
  );
}

/**
 * Where to read public-filter for `sha`: the commit itself, or, for commits
 * from before the split (the one the first run starts from), the first commit
 * that added the filter. That choice never changes, so the drift check
 * recomputes the split's tree with the same filter the split was built with.
 */
function filterFor(sha: string): string {
  if (hasFilter(sha)) {
    return sha;
  }
  firstFilterCommit ??= lines(
    git(
      'rev-list',
      '--reverse',
      '--first-parent',
      args.source,
      '--',
      FILTER_PATH,
    ),
  )[0];
  if (firstFilterCommit === undefined) {
    throw new Error(`${FILTER_PATH} is not in ${args.source}'s history.`);
  }
  // Only commits from before the filter existed may borrow it. A later commit
  // without one deleted or moved it; publishing it under stale rules could
  // leak private files, so stop instead.
  if (isAncestor(firstFilterCommit, sha)) {
    throw new Error(
      `${sha} has no ${FILTER_PATH}, but ${firstFilterCommit} added it. ` +
        'Restore the filter before syncing.',
    );
  }
  return firstFilterCommit;
}

/** The first capture group of `re` on each line of `body` it matches. */
function matchLines(body: string[], re: RegExp): string[] {
  return body.flatMap(l => {
    const m = re.exec(l.trim());
    return m ? [m[1]] : [];
  });
}

function hasFilter(sha: string): boolean {
  return git('ls-tree', '--name-only', sha, '--', FILTER_PATH) !== '';
}

function treeOf(commit: string): string {
  return git('rev-parse', `${commit}^{tree}`);
}

function isAncestor(a: string, b: string): boolean {
  try {
    git('merge-base', '--is-ancestor', a, b);
    return true;
  } catch {
    return false;
  }
}

/**
 * Squash-merge titles end in `(#123)`, a mono-internal PR number. On mono
 * that autolinks to an unrelated public PR (or a 404), so drop it. Any other
 * bare `#123` in the title is a mono-internal number too; spell it
 * `mono-internal#123`, which GitHub doesn't autolink.
 */
function publicSubject(subject: string): string {
  return subject.replace(PR_SUFFIX, '').replace(BARE_REF, 'mono-internal#$1');
}
