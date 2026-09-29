#!/usr/bin/env node
// oxlint-disable no-console
// Duplicate a GitHub repo (full git history + most settings) without forking.
//
// Usage: node gh-duplicate-repo.ts SRC_OWNER/REPO DST_OWNER/REPO
//          [--private|--public|--internal] [--dry-run] [--skip-env NAME]...
//
// --skip-env leaves an environment (and its secrets) out; repeat it for more.
//
// --dry-run makes every read the real run makes, so missing permissions show
// up, but only prints the writes (API POST/PUT/PATCH, git clone/push).
//
// Env:
//   GITHUB_TOKEN           token to use (defaults to `gh auth token`)
//   COPY_COLLABORATORS=1   also copy direct collaborators (SENDS INVITES) and teams
//
// Requires: Node 22.12+, git, gh (for the token and git credential helper).
// Doesn't copy Git LFS objects; mono doesn't use LFS.
//
// Issues, wiki and discussions are turned OFF on the destination.
// Actions is DISABLED on the destination during and after the copy so the mirror push
// doesn't trigger workflows. Re-add secrets, then re-enable (the script prints the command).

import {execFileSync} from 'node:child_process';
import {mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {parseArgs} from 'node:util';

const {values: flags, positionals} = parseArgs({
  allowPositionals: true,
  options: {
    'private': {type: 'boolean', default: false},
    'public': {type: 'boolean', default: false},
    'internal': {type: 'boolean', default: false},
    'dry-run': {type: 'boolean', default: false},
    'skip-env': {type: 'string', multiple: true, default: []},
  },
});
const [src, dst] = positionals;
const visibilities = (['private', 'public', 'internal'] as const).filter(
  v => flags[v],
);
if (
  positionals.length !== 2 ||
  !src?.includes('/') ||
  !dst?.includes('/') ||
  visibilities.length > 1
) {
  console.error(
    'Usage: gh-duplicate-repo.ts SRC_OWNER/REPO DST_OWNER/REPO ' +
      '[--private|--public|--internal] [--dry-run] [--skip-env NAME]...',
  );
  process.exit(2);
}
const visibility = visibilities[0] ?? 'private';
const dryRun = flags['dry-run'];
const [dstOwner, dstName] = dst.split('/');
const copyCollaborators = process.env.COPY_COLLABORATORS === '1';

// --- helpers -------------------------------------------------------------------------

const token =
  process.env.GITHUB_TOKEN ??
  execFileSync('gh', ['auth', 'token'], {encoding: 'utf8'}).trim();

const API = 'https://api.github.com';
const NEXT_LINK = /<([^>]+)>;\s*rel="next"/;
const headers: Record<string, string> = {
  'Authorization': `Bearer ${token}`,
  'Accept': 'application/vnd.github+json',
  'X-GitHub-Api-Version': '2022-11-28',
};

class HttpError extends Error {
  readonly status: number;
  constructor(status: number, method: string, url: string, body: string) {
    super(`${method} ${url} -> ${status}: ${body.slice(0, 300)}`);
    this.status = status;
  }
}

async function request(
  method: string,
  url: string,
  body?: unknown,
): Promise<Response> {
  const res = await fetch(url.startsWith('http') ? url : `${API}/${url}`, {
    method,
    headers:
      body === undefined
        ? headers
        : {...headers, 'Content-Type': 'application/json'},
    body: body === undefined ? null : JSON.stringify(body),
  });
  if (!res.ok) {
    throw new HttpError(res.status, method, url, await res.text());
  }
  return res;
}

async function api<T = unknown>(
  method: string,
  path: string,
  body?: unknown,
): Promise<T> {
  if (dryRun && method !== 'GET') {
    // Variable values can be sensitive, and a dry run may log to CI.
    const shown =
      body === undefined
        ? ''
        : ` ${JSON.stringify(body, (k, v) => (k === 'value' ? '<redacted>' : v))}`;
    console.log(`   would ${method} ${path}${shown}`);
    return null as T;
  }
  const res = await request(method, path, body);
  // Several PUT/POST endpoints answer 204 No Content, which json() rejects.
  return (res.status === 204 ? null : await res.json()) as T;
}

function get<T>(path: string): Promise<T> {
  return api<T>('GET', path);
}

/**
 * Follows Link rel="next". `key` extracts the array from wrapper responses
 * like {variables: [...]}.
 */
async function getAll<T>(path: string, key?: string): Promise<T[]> {
  if (dryRun) {
    // Report every unreadable list instead of stopping at the first one.
    try {
      return await getAllPages<T>(path, key);
    } catch (e) {
      if (e instanceof HttpError) {
        warn(
          `cannot read ${path} (${e.status}); the real run would fail this read`,
        );
        return [];
      }
      throw e;
    }
  }
  return getAllPages<T>(path, key);
}

async function getAllPages<T>(path: string, key?: string): Promise<T[]> {
  const out: T[] = [];
  let url: string | undefined =
    `${path}${path.includes('?') ? '&' : '?'}per_page=100`;
  while (url) {
    const res = await request('GET', url);
    const data = (await res.json()) as T[] | Record<string, T[]>;
    out.push(...(key ? (data as Record<string, T[]>)[key] : (data as T[])));
    url = res.headers.get('link')?.match(NEXT_LINK)?.[1];
  }
  return out;
}

/** For endpoints that answer 204 when a feature is on and 404 when off. */
async function isOn(path: string): Promise<boolean> {
  try {
    await request('GET', path);
    return true;
  } catch (e) {
    if (e instanceof HttpError && e.status === 404) {
      return false;
    }
    throw e;
  }
}

const enc = encodeURIComponent;
const step = (msg: string) => console.log(`==> ${msg}`);
const warn = (msg: string) => console.error(`   ! ${msg}`);

// Steps that failed but didn't stop the run. They are listed again at the end
// and make the exit status nonzero, so a partial copy doesn't pass for done.
const failed: string[] = [];

async function attempt<T>(
  label: string,
  fn: () => T | Promise<T>,
): Promise<T | undefined> {
  try {
    return await fn();
  } catch (e) {
    warn(`${label}: ${e instanceof Error ? e.message : String(e)}`);
    failed.push(label);
    return undefined;
  }
}

// Uses gh as git credential helper so no `gh auth setup-git` is needed and the
// token never appears in a URL.
const gitAuth = [
  '-c',
  'credential.helper=',
  '-c',
  'credential.helper=!gh auth git-credential',
];
const git = (...args: string[]) => {
  if (dryRun) {
    console.log(`   would run git ${args.join(' ')}`);
    return;
  }
  execFileSync('git', [...gitAuth, ...args], {stdio: 'inherit'});
};

// --- GitHub API shapes (only the fields used here) -------------------------------------

type Repo = {description: string | null} & Record<string, unknown>;
type Label = {name: string; color: string; description: string | null};
type Autolink = {
  key_prefix: string;
  url_template: string;
  is_alphanumeric: boolean;
};
type Ruleset = {
  id: number;
  name: string;
  target: string;
  enforcement: string;
  bypass_actors?: unknown[] | undefined;
  conditions?: unknown;
  rules?: unknown[] | undefined;
};
type Enabled = {enabled: boolean} | undefined;
type BranchProtection = {
  required_status_checks?:
    | {strict: boolean; checks: unknown[]}
    | null
    | undefined;
  required_pull_request_reviews?:
    | {
        dismiss_stale_reviews: boolean;
        require_code_owner_reviews: boolean;
        required_approving_review_count: number;
        require_last_push_approval: boolean;
      }
    | undefined;
  restrictions?:
    | {
        users?: {login: string}[] | undefined;
        teams?: {slug: string}[] | undefined;
        apps?: {slug: string}[] | undefined;
      }
    | undefined;
} & Record<string, Enabled | unknown>;
type Variable = {name: string; value: string};
type ProtectionRule = {
  type: string;
  wait_timer?: number | undefined;
  prevent_self_review?: boolean | undefined;
  reviewers?: {type: string; reviewer: {id: number}}[] | undefined;
};
type Environment = {
  name: string;
  can_admins_bypass?: boolean | undefined;
  protection_rules?: ProtectionRule[] | undefined;
  deployment_branch_policy?:
    | {custom_branch_policies: boolean}
    | null
    | undefined;
};
type Named = {name: string};
type Hook = {id: number; config: {url: string}; events: string[]};
type DeployKey = {title: string; read_only: boolean};

// --- main ----------------------------------------------------------------------------

const tmp = mkdtempSync(join(tmpdir(), 'dup-repo-'));
try {
  await main();
  if (failed.length) {
    console.error(
      `\n==> ${failed.length} step(s) failed; redo them by hand:\n` +
        failed.map(f => `   ${f}`).join('\n'),
    );
    process.exitCode = 1;
  }
} finally {
  rmSync(tmp, {recursive: true, force: true});
}

async function main() {
  const srcRepo = await get<Repo>(`repos/${src}`);
  const me = (await get<{login: string}>('user')).login;

  // --- create ------------------------------------------------------------------------
  step(`Creating ${dst}`);
  if (dryRun) {
    try {
      await get(`repos/${dst}`);
      warn(`${dst} already exists; the real run would fail here`);
    } catch (e) {
      if (!(e instanceof HttpError && e.status === 404)) {
        throw e;
      }
    }
  }
  if (dstOwner.toLowerCase() === me.toLowerCase()) {
    if (visibility === 'internal') {
      // Only organization repos can be internal; don't quietly go private.
      throw new Error(`--internal needs an organization; ${dst} is yours.`);
    }
    await api('POST', 'user/repos', {
      name: dstName,
      description: srcRepo.description ?? '',
      private: visibility !== 'public',
    });
  } else {
    await api('POST', `orgs/${dstOwner}/repos`, {
      name: dstName,
      description: srcRepo.description ?? '',
      visibility,
    });
  }

  step(`Disabling Actions on ${dst}`);
  await api('PUT', `repos/${dst}/actions/permissions`, {enabled: false});

  // --- git ---------------------------------------------------------------------------
  step('Mirroring git history');
  const bare = join(tmp, 'repo.git');
  const dstUrl = `https://github.com/${dst}.git`;
  git('clone', '--bare', `https://github.com/${src}.git`, bare);
  try {
    git('-C', bare, 'push', '--mirror', dstUrl);
  } catch (e) {
    // The new repo is covered by org rulesets targeting the default branch
    // from the moment it exists.
    warn(
      `push to ${dst} failed. If the error names a ruleset (e.g. "changes ` +
        `must be made through a pull request"), run this as a bypass actor ` +
        `of the org rulesets, or exclude ${dst} from them for the copy.`,
    );
    throw e;
  }

  // --- repo settings -----------------------------------------------------------------
  step('Repo settings');
  const settingKeys = [
    'homepage',
    'has_projects',
    'is_template',
    'allow_merge_commit',
    'allow_squash_merge',
    'allow_rebase_merge',
    'allow_auto_merge',
    'allow_update_branch',
    'delete_branch_on_merge',
    'squash_merge_commit_title',
    'squash_merge_commit_message',
    'merge_commit_title',
    'merge_commit_message',
    'web_commit_signoff_required',
    'default_branch',
  ];
  const settings: Record<string, unknown> = Object.fromEntries(
    settingKeys
      .filter(k => srcRepo[k] !== null && srcRepo[k] !== undefined)
      .map(k => [k, srcRepo[k]]),
  );
  Object.assign(settings, {
    has_issues: false,
    has_wiki: false,
    has_discussions: false,
  });
  await api('PATCH', `repos/${dst}`, settings);

  step('Security settings');
  // Without admin, vulnerability-alerts answers 404 whether alerts are on or
  // off, so only trust it for admins.
  const isAdmin =
    (srcRepo.permissions as {admin?: boolean} | undefined)?.admin === true;
  // Dependabot alerts first: Dependabot security updates need them on.
  if (!isAdmin) {
    warn(`Dependabot alerts of ${src} not readable (needs admin)`);
  } else if (await isOn(`repos/${src}/vulnerability-alerts`)) {
    await attempt('Dependabot alerts', () =>
      api('PUT', `repos/${dst}/vulnerability-alerts`),
    );
  }
  // Only admins see security_and_analysis. On a private repo, secret and code
  // scanning also need a GitHub Advanced Security license, or this is refused.
  const analysis = srcRepo.security_and_analysis as
    | Record<string, {status: string}>
    | undefined;
  if (analysis) {
    // One feature per request: if the destination refuses one (say, secret
    // scanning without a license), the others still get copied.
    for (const [feature, {status}] of Object.entries(analysis)) {
      await attempt(`security_and_analysis.${feature}`, () =>
        api('PATCH', `repos/${dst}`, {
          security_and_analysis: {[feature]: {status}},
        }),
      );
    }
  } else {
    warn(`security_and_analysis of ${src} not readable (needs admin)`);
  }
  const reporting = await attempt('private vulnerability reporting', () =>
    get<{enabled: boolean}>(`repos/${src}/private-vulnerability-reporting`),
  );
  if (reporting?.enabled) {
    await attempt('private vulnerability reporting', () =>
      api('PUT', `repos/${dst}/private-vulnerability-reporting`),
    );
  }
  // A 404 here just means code scanning isn't set up, not a failed step.
  const setup = await attempt('code scanning default setup', async () => {
    try {
      return await get<{
        state: string;
        languages: string[];
        query_suite: string;
      }>(`repos/${src}/code-scanning/default-setup`);
    } catch (e) {
      if (e instanceof HttpError && e.status === 404) {
        return undefined;
      }
      throw e;
    }
  });
  if (setup?.state === 'configured') {
    await attempt('code scanning default setup', () =>
      api('PATCH', `repos/${dst}/code-scanning/default-setup`, {
        state: 'configured',
        languages: setup.languages,
        query_suite: setup.query_suite,
      }),
    );
  }

  step('Topics');
  const {names} = await get<{names: string[]}>(`repos/${src}/topics`);
  await api('PUT', `repos/${dst}/topics`, {names});

  step('Labels (upsert; default labels not in source are left alone)');
  for (const {name, color, description} of await getAll<Label>(
    `repos/${src}/labels`,
  )) {
    const body = {
      name,
      color,
      ...(description === null ? {} : {description}),
    };
    try {
      await api('POST', `repos/${dst}/labels`, body);
    } catch {
      await attempt(`label ${name}`, () =>
        api('PATCH', `repos/${dst}/labels/${enc(name)}`, body),
      );
    }
  }

  step('Autolinks');
  for (const {
    key_prefix,
    url_template,
    is_alphanumeric,
  } of await getAll<Autolink>(`repos/${src}/autolinks`)) {
    await attempt(`autolink ${key_prefix}`, () =>
      api('POST', `repos/${dst}/autolinks`, {
        key_prefix,
        url_template,
        is_alphanumeric,
      }),
    );
  }

  // --- rules -------------------------------------------------------------------------
  step('Rulesets (repo-level only)');
  for (const {id} of await getAll<Ruleset>(
    `repos/${src}/rulesets?includes_parents=false`,
  )) {
    const r = await get<Ruleset>(`repos/${src}/rulesets/${id}`);
    const {name, target, enforcement, bypass_actors, conditions, rules} = r;
    await attempt(`ruleset ${name}`, () =>
      api('POST', `repos/${dst}/rulesets`, {
        name,
        target,
        enforcement,
        bypass_actors,
        conditions,
        rules,
      }),
    );
  }

  step('Classic branch protection');
  for (const {name: branch} of await getAll<Named>(
    `repos/${src}/branches?protected=true`,
  )) {
    let p: BranchProtection;
    try {
      p = await get<BranchProtection>(
        `repos/${src}/branches/${enc(branch)}/protection`,
      );
    } catch (e) {
      if (e instanceof HttpError && e.status === 404) {
        continue; // protected by rulesets only
      }
      throw e;
    }
    const flag = (k: string) => (p[k] as Enabled)?.enabled ?? false;
    const rpr = p.required_pull_request_reviews;
    const body = {
      required_status_checks: p.required_status_checks
        ? {
            strict: p.required_status_checks.strict,
            checks: p.required_status_checks.checks,
          }
        : null,
      enforce_admins: flag('enforce_admins'),
      required_pull_request_reviews: rpr
        ? {
            dismiss_stale_reviews: rpr.dismiss_stale_reviews,
            require_code_owner_reviews: rpr.require_code_owner_reviews,
            required_approving_review_count:
              rpr.required_approving_review_count,
            require_last_push_approval: rpr.require_last_push_approval,
          }
        : null,
      restrictions: p.restrictions
        ? {
            users: (p.restrictions.users ?? []).map(u => u.login),
            teams: (p.restrictions.teams ?? []).map(t => t.slug),
            apps: (p.restrictions.apps ?? []).map(a => a.slug),
          }
        : null,
      required_linear_history: flag('required_linear_history'),
      allow_force_pushes: flag('allow_force_pushes'),
      allow_deletions: flag('allow_deletions'),
      block_creations: flag('block_creations'),
      required_conversation_resolution: flag(
        'required_conversation_resolution',
      ),
      lock_branch: flag('lock_branch'),
      allow_fork_syncing: flag('allow_fork_syncing'),
    };
    await attempt(`protection ${branch}`, () =>
      api('PUT', `repos/${dst}/branches/${enc(branch)}/protection`, body),
    );
  }

  // --- actions -----------------------------------------------------------------------
  step('Actions variables');
  for (const {name, value} of await getAll<Variable>(
    `repos/${src}/actions/variables`,
    'variables',
  )) {
    await attempt(`variable ${name}`, () =>
      api('POST', `repos/${dst}/actions/variables`, {name, value}),
    );
  }

  step('Actions workflow permissions');
  await attempt('workflow permissions', async () =>
    api(
      'PUT',
      `repos/${dst}/actions/permissions/workflow`,
      await get(`repos/${src}/actions/permissions/workflow`),
    ),
  );

  step('Environments');
  const skipped = new Set(flags['skip-env']);
  const environments = (
    await getAll<Environment>(`repos/${src}/environments`, 'environments')
  ).filter(env => !skipped.has(env.name));
  if (skipped.size) {
    console.log(`   skipping ${[...skipped].join(', ')}`);
  }
  for (const env of environments) {
    const en = enc(env.name);
    const rules = env.protection_rules ?? [];
    const reviewersRule = rules.find(r => r.type === 'required_reviewers');
    const ok = await attempt(`environment ${env.name}`, async () => {
      await api('PUT', `repos/${dst}/environments/${en}`, {
        wait_timer: rules.find(r => r.type === 'wait_timer')?.wait_timer ?? 0,
        prevent_self_review: reviewersRule?.prevent_self_review ?? false,
        reviewers: (reviewersRule?.reviewers ?? []).map(r => ({
          type: r.type,
          id: r.reviewer.id,
        })),
        deployment_branch_policy: env.deployment_branch_policy ?? null,
        can_admins_bypass: env.can_admins_bypass ?? true,
      });
      return true;
    });
    if (!ok) {
      continue;
    }

    if (env.deployment_branch_policy?.custom_branch_policies) {
      const policies = await getAll<{name: string; type: string}>(
        `repos/${src}/environments/${en}/deployment-branch-policies`,
        'branch_policies',
      );
      for (const {name, type} of policies) {
        await attempt(`branch policy ${env.name}/${name}`, () =>
          api(
            'POST',
            `repos/${dst}/environments/${en}/deployment-branch-policies`,
            {name, type},
          ),
        );
      }
    }

    for (const {name, value} of await getAll<Variable>(
      `repos/${src}/environments/${en}/variables`,
      'variables',
    )) {
      await attempt(`variable ${env.name}/${name}`, () =>
        api('POST', `repos/${dst}/environments/${en}/variables`, {
          name,
          value,
        }),
      );
    }
  }

  // --- access ------------------------------------------------------------------------
  if (copyCollaborators) {
    step('Direct collaborators (sends invitations)');
    const roleMap: Record<string, string> = {write: 'push', read: 'pull'};
    for (const {login, role_name} of await getAll<{
      login: string;
      role_name: string;
    }>(`repos/${src}/collaborators?affiliation=direct`)) {
      await attempt(`collaborator ${login}`, () =>
        api('PUT', `repos/${dst}/collaborators/${login}`, {
          permission: roleMap[role_name] ?? role_name,
        }),
      );
    }

    step('Teams (same org only)');
    for (const {slug, permission} of await getAll<{
      slug: string;
      permission: string;
    }>(`repos/${src}/teams`)) {
      await attempt(`team ${slug}`, () =>
        api('PUT', `orgs/${dstOwner}/teams/${slug}/repos/${dst}`, {
          permission,
        }),
      );
    }
  }

  // --- report what needs manual work -------------------------------------------------
  console.log('\n==> Manual follow-up');
  const list = async (label: string, fn: () => Promise<string[]>) => {
    // Heading first, so a read warning lands under the list it belongs to.
    console.log(`${label}:`);
    const items = (await attempt(label, fn)) ?? [];
    for (const i of items) {
      console.log(`   ${i}`);
    }
  };

  await list('Actions secrets to re-add', async () => {
    const out = (
      await getAll<Named>(`repos/${src}/actions/secrets`, 'secrets')
    ).map(s => s.name);
    for (const env of environments) {
      const s = await getAll<Named>(
        `repos/${src}/environments/${enc(env.name)}/secrets`,
        'secrets',
      );
      out.push(...s.map(x => `[env ${env.name}] ${x.name}`));
    }
    return out;
  });
  await list('Dependabot secrets', async () =>
    (await getAll<Named>(`repos/${src}/dependabot/secrets`, 'secrets')).map(
      s => s.name,
    ),
  );
  // Only the host: a webhook URL's path or query can itself be the secret,
  // and this output may end up in CI logs. The settings page has the rest.
  await list('Webhooks (recreate; secrets not readable)', async () =>
    (await getAll<Hook>(`repos/${src}/hooks`)).map(
      h =>
        `${URL.parse(h.config.url)?.host ?? '(unparsable URL)'} ` +
        `events=${h.events.join(',')} ` +
        `https://github.com/${src}/settings/hooks/${h.id}`,
    ),
  );
  await list(
    'Deploy keys (re-add if you still hold the private keys)',
    async () =>
      (await getAll<DeployKey>(`repos/${src}/keys`)).map(
        k => `${k.title} read_only=${k.read_only}`,
      ),
  );

  const perm = await attempt('actions permissions', () =>
    get<{allowed_actions?: string | undefined}>(
      `repos/${src}/actions/permissions`,
    ),
  );
  console.log(`\nActions is DISABLED on ${dst}.`);
  const allowed = perm?.allowed_actions;
  if (allowed === undefined) {
    // Guessing 'all' could allow more actions than the source does.
    warn(
      `couldn't read ${src}'s allowed actions; check them in its settings ` +
        `and re-enable ${dst} with the same policy by hand`,
    );
    return;
  }
  console.log('After adding secrets, re-enable it with:');
  console.log(
    `   gh api -X PUT repos/${dst}/actions/permissions -F enabled=true -f allowed_actions=${allowed}`,
  );
  if (allowed === 'selected') {
    // Read in both modes, so a dry run exercises the same endpoint.
    const selected = await get(
      `repos/${src}/actions/permissions/selected-actions`,
    );
    if (dryRun) {
      console.log('   (a real run also writes selected-actions.json here)');
      return;
    }
    writeFileSync('selected-actions.json', JSON.stringify(selected, null, 2));
    console.log(
      `   gh api -X PUT repos/${dst}/actions/permissions/selected-actions --input selected-actions.json`,
    );
  }
}
