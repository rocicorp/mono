#!/usr/bin/env node

/* eslint-disable no-console */
import {execFile as execFileCallback} from 'node:child_process';
import {writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {promisify} from 'node:util';

const execFile = promisify(execFileCallback);

const DEFAULT_LOCKFILES = ['package-lock.json'];
const DEPENDENCY_FIELDS = [
  'dependencies',
  'devDependencies',
  'optionalDependencies',
  'peerDependencies',
] as const;
const NOTION_API_BASE_URL = 'https://api.notion.com/v1';
const NOTION_API_VERSION = '2022-06-28';
const OSV_BATCH_QUERY_URL = 'https://api.osv.dev/v1/querybatch';
const GITHUB_ADVISORY_BASE_URL = 'https://github.com/advisories';
const NVD_CVE_BASE_URL = 'https://nvd.nist.gov/vuln/detail';
const SEMVER_VERSION_REGEX = /^v?\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/;
const LEADING_V_REGEX = /^v/;
const GITHUB_HTTPS_REMOTE_REGEX =
  /^https:\/\/github\.com\/([^/]+\/[^/]+?)(\.git)?$/;
const GITHUB_SSH_REMOTE_REGEX = /^git@github\.com:([^/]+\/[^/]+?)(\.git)?$/;

const options = parseArgs(process.argv.slice(2));
const [repoRoot, githubCommitBaseUrl] = await Promise.all([
  detectRepoRoot(),
  detectGithubCommitBaseUrl(options.githubRepo),
]);

type DependencyField = (typeof DEPENDENCY_FIELDS)[number];

type OutputFormat = 'tsv' | 'notion' | 'html' | 'md' | 'json';

type PackageEntry = {
  name?: string | undefined;
  version?: string | undefined;
  resolved?: string | undefined;
  link?: boolean | undefined;
  dependencies?: Record<string, string> | undefined;
  devDependencies?: Record<string, string> | undefined;
  optionalDependencies?: Record<string, string> | undefined;
  peerDependencies?: Record<string, string> | undefined;
};

type Lockfile = {
  packages?: Record<string, PackageEntry | undefined> | undefined;
};

type Commit = {
  sha: string;
  date: string;
  subject: string;
};

type DependencyChange = {
  sha: string;
  date: string;
  subject: string;
  lockfilePath: string;
  workspacePath: string;
  section: DependencyField;
  dependency: string;
  change: 'added' | 'updated';
  specBefore: string | undefined;
  specAfter: string;
  resolvedBefore: string | undefined;
  resolvedAfter: string | undefined;
};

type AnnotatedDependencyChange = DependencyChange & {
  knownVulnsBefore: string;
  knownVulnsAfter: string;
  presentDateStart: string;
  presentDateEnd: string;
  presentCommitStart: string;
  presentCommitEnd: string;
};

type DedupedChangeKey = {
  sha: string;
  lockfilePath: string;
  dependency: string;
  change: DependencyChange['change'];
  specBefore: string;
  specAfter: string;
  resolvedBefore: string;
  resolvedAfter: string;
};

type Options = {
  since: string;
  lockfiles: string[];
  output: OutputFormat;
  allDeps: boolean;
  htmlFile: string;
  mdFile: string;
  jsonFile: string;
  notionParentPageId?: string | undefined;
  notionDatabaseTitle: string;
  githubRepo?: string | undefined;
};

type NotionRichText = {
  type: 'text';
  text: {
    content: string;
  };
};

type NotionRequestOptions = {
  method?: 'GET' | 'POST';
  body?: unknown;
};

type NotionDatabaseResponse = {
  id: string;
  url: string;
};

type OsvQuery = {
  package: {
    ecosystem: 'npm';
    name: string;
  };
  version: string;
};

type OsvBatchRequest = {
  queries: OsvQuery[];
};

type OsvVulnerability = {
  id: string;
  aliases?: string[] | undefined;
};

type OsvBatchResult = {
  vulns?: OsvVulnerability[] | undefined;
};

type OsvBatchResponse = {
  results: OsvBatchResult[];
};

type PackageVersion = {
  dependency: string;
  version: string;
};

type PresenceRangeAccumulator = {
  startOrder?: number | undefined;
  startDate?: string | undefined;
  startSha?: string | undefined;
  lastAfterOrder?: number | undefined;
  lastAfterDate?: string | undefined;
  lastAfterSha?: string | undefined;
  lastBeforeOrder?: number | undefined;
  lastBeforeDate?: string | undefined;
  lastBeforeSha?: string | undefined;
};

type PresenceRange = {
  dateStart: string;
  dateEnd: string;
  commitStart: string;
  commitEnd: string;
  startKnown: boolean;
  endKnown: boolean;
};

function parseArgs(argv: string[]): Options {
  const lockfiles: string[] = [];
  let since = '6 months ago';
  let output: OutputFormat = 'html';
  let allDeps = false;
  let htmlFile = 'package-lock-commit-deps-report.html';
  let mdFile = 'package-lock-commit-deps-report.md';
  let jsonFile = 'package-lock-commit-deps-report.json';
  let notionParentPageId: string | undefined;
  let notionDatabaseTitle: string | undefined;
  let githubRepo: string | undefined;

  for (const arg of argv) {
    if (arg === '--help' || arg === '-h') {
      printHelp();
      process.exit(0);
    }

    if (arg === '--notion') {
      output = 'notion';
      continue;
    }

    if (arg === '--all-deps') {
      allDeps = true;
      continue;
    }

    if (arg.startsWith('--since=')) {
      since = arg.slice('--since='.length);
      continue;
    }

    if (arg.startsWith('--lockfile=')) {
      lockfiles.push(arg.slice('--lockfile='.length));
      continue;
    }

    if (arg.startsWith('--output=')) {
      const value = arg.slice('--output='.length);
      if (
        value !== 'tsv' &&
        value !== 'notion' &&
        value !== 'html' &&
        value !== 'md' &&
        value !== 'json'
      ) {
        throw new Error(`Unknown output format: ${value}`);
      }
      output = value;
      continue;
    }

    if (arg.startsWith('--html-file=')) {
      htmlFile = arg.slice('--html-file='.length);
      continue;
    }

    if (arg.startsWith('--md-file=')) {
      mdFile = arg.slice('--md-file='.length);
      continue;
    }

    if (arg.startsWith('--json-file=')) {
      jsonFile = arg.slice('--json-file='.length);
      continue;
    }

    if (arg.startsWith('--notion-parent-page-id=')) {
      notionParentPageId = arg.slice('--notion-parent-page-id='.length);
      continue;
    }

    if (arg.startsWith('--notion-database-title=')) {
      notionDatabaseTitle = arg.slice('--notion-database-title='.length);
      continue;
    }

    if (arg.startsWith('--github-repo=')) {
      githubRepo = arg.slice('--github-repo='.length);
      continue;
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return {
    since,
    lockfiles: lockfiles.length > 0 ? lockfiles : DEFAULT_LOCKFILES,
    output,
    allDeps,
    htmlFile,
    mdFile,
    jsonFile,
    notionParentPageId,
    notionDatabaseTitle:
      notionDatabaseTitle ?? `Package lock dependency changes (${since})`,
    githubRepo,
  };
}

function printHelp(): void {
  console.log(`Print added or updated direct dependencies from commits that touched package-lock files.

Default output: HTML report written to package-lock-commit-deps-report.html.
Default dependency filter: vulnerable deps only. Use --all-deps to include all.

Usage:
  node --experimental-strip-types src/package-lock-commit-deps.ts [--since=<git-date>] [--lockfile=<path> ...]
  node --experimental-strip-types src/package-lock-commit-deps.ts [--all-deps]
  node --experimental-strip-types src/package-lock-commit-deps.ts --output=notion --notion-parent-page-id=<page-id> [--notion-database-title=<title>]
  node --experimental-strip-types src/package-lock-commit-deps.ts --output=html [--html-file=<path>]
  node --experimental-strip-types src/package-lock-commit-deps.ts --output=md [--md-file=<path>]
  node --experimental-strip-types src/package-lock-commit-deps.ts --output=json [--json-file=<path>]

Environment:
  NOTION_TOKEN   Required when --output=notion or --notion is used

Examples:
  pnpm --filter verify-package-deps run package-lock-commit-deps
  pnpm --filter verify-package-deps run package-lock-commit-deps -- --since='3 months ago'
  pnpm --filter verify-package-deps run package-lock-commit-deps -- --all-deps
  pnpm --filter verify-package-deps run package-lock-commit-deps -- --lockfile=package-lock.json
  pnpm --filter verify-package-deps run package-lock-commit-deps -- --notion --notion-parent-page-id=<page-id>
  pnpm --filter verify-package-deps run package-lock-commit-deps -- --output=html --html-file=dependency-report.html
  pnpm --filter verify-package-deps run package-lock-commit-deps -- --output=md --md-file=dependency-report.md
  pnpm --filter verify-package-deps run package-lock-commit-deps -- --output=json --json-file=dependency-report.json
`);
}

async function detectRepoRoot(): Promise<string> {
  const {stdout} = await execFile('git', ['rev-parse', '--show-toplevel'], {
    cwd: process.cwd(),
  });
  return stdout.trim();
}

async function detectGithubCommitBaseUrl(
  githubRepo: string | undefined,
): Promise<string | undefined> {
  if (githubRepo) {
    return `https://github.com/${githubRepo}/commit`;
  }
  try {
    const {stdout} = await execFile('git', ['remote', 'get-url', 'origin'], {
      cwd: process.cwd(),
    });
    const remote = stdout.trim();
    // https://github.com/owner/repo(.git)
    const httpsMatch = remote.match(GITHUB_HTTPS_REMOTE_REGEX);
    if (httpsMatch) {
      return `https://github.com/${httpsMatch[1]}/commit`;
    }
    // git@github.com:owner/repo(.git)
    const sshMatch = remote.match(GITHUB_SSH_REMOTE_REGEX);
    if (sshMatch) {
      return `https://github.com/${sshMatch[1]}/commit`;
    }
  } catch {
    // No remote or not GitHub
  }
  return undefined;
}

async function runGit(args: string[]): Promise<string> {
  const {stdout} = await execFile('git', ['--no-pager', ...args], {
    cwd: repoRoot,
    maxBuffer: 64 * 1024 * 1024,
  });
  return stdout;
}

async function getCommits(
  since: string,
  lockfiles: string[],
): Promise<Commit[]> {
  const output = await runGit([
    'log',
    `--since=${since}`,
    '--format=%H%x09%cs%x09%s',
    '--',
    ...lockfiles,
  ]);

  return output
    .split('\n')
    .filter(Boolean)
    .map(line => {
      const [sha, date, subject] = line.split('\t');
      if (!sha || !date || subject === undefined) {
        throw new Error(`Unexpected git log line: ${line}`);
      }
      return {sha, date, subject};
    });
}

async function getTouchedLockfiles(
  sha: string,
  lockfiles: string[],
): Promise<string[]> {
  const output = await runGit([
    'show',
    '--format=',
    '--name-only',
    sha,
    '--',
    ...lockfiles,
  ]);

  return output.split('\n').filter(Boolean);
}

async function getFileAtRevision(
  revision: string,
  path: string,
): Promise<string | undefined> {
  try {
    return await runGit(['show', `${revision}:${path}`]);
  } catch {
    return undefined;
  }
}

function parseLockfile(
  content: string | undefined,
  revision: string,
  path: string,
): Lockfile {
  if (content === undefined) {
    return {};
  }

  try {
    return JSON.parse(content) as Lockfile;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to parse ${path} at ${revision}: ${message}`);
  }
}

function isWorkspacePath(path: string): boolean {
  return path === '' || !path.includes('node_modules/');
}

function getWorkspaceDisplayPath(path: string): string {
  return path === '' ? '.' : path;
}

function formatResolution(entry: PackageEntry | undefined): string | undefined {
  if (!entry) {
    return undefined;
  }

  if (entry.link) {
    return entry.resolved ? `link:${entry.resolved}` : 'link';
  }

  return entry.version ?? entry.resolved;
}

function resolveDependencyVersion(
  lockfile: Lockfile,
  workspacePath: string,
  dependencyName: string,
): string | undefined {
  const packages = lockfile.packages ?? {};
  const candidatePaths =
    workspacePath === ''
      ? [`node_modules/${dependencyName}`]
      : [
          `${workspacePath}/node_modules/${dependencyName}`,
          `node_modules/${dependencyName}`,
        ];

  for (const candidatePath of candidatePaths) {
    const resolution = formatResolution(packages[candidatePath]);
    if (resolution !== undefined) {
      return resolution;
    }
  }

  for (const [packagePath, entry] of Object.entries(packages)) {
    if (isWorkspacePath(packagePath) && entry?.name === dependencyName) {
      const version = formatResolution(entry);
      return version
        ? `workspace:${getWorkspaceDisplayPath(packagePath)}@${version}`
        : `workspace:${getWorkspaceDisplayPath(packagePath)}`;
    }
  }

  return undefined;
}

function collectDependencyChanges(
  commit: Commit,
  lockfilePath: string,
  before: Lockfile,
  after: Lockfile,
): DependencyChange[] {
  const changes: DependencyChange[] = [];
  const beforePackages = before.packages ?? {};
  const afterPackages = after.packages ?? {};
  const workspacePaths = new Set<string>([
    ...Object.keys(beforePackages).filter(isWorkspacePath),
    ...Object.keys(afterPackages).filter(isWorkspacePath),
  ]);

  for (const workspacePath of [...workspacePaths].toSorted()) {
    const beforePackage = beforePackages[workspacePath] ?? {};
    const afterPackage = afterPackages[workspacePath] ?? {};

    for (const section of DEPENDENCY_FIELDS) {
      const beforeDependencies = beforePackage[section] ?? {};
      const afterDependencies = afterPackage[section] ?? {};
      const dependencyNames = new Set<string>([
        ...Object.keys(beforeDependencies),
        ...Object.keys(afterDependencies),
      ]);

      for (const dependency of [...dependencyNames].toSorted()) {
        const specBefore = beforeDependencies[dependency];
        const specAfter = afterDependencies[dependency];
        const resolvedBefore = resolveDependencyVersion(
          before,
          workspacePath,
          dependency,
        );
        const resolvedAfter = resolveDependencyVersion(
          after,
          workspacePath,
          dependency,
        );

        if (specAfter === undefined) {
          continue;
        }

        if (specBefore === specAfter && resolvedBefore === resolvedAfter) {
          continue;
        }

        changes.push({
          sha: commit.sha,
          date: commit.date,
          subject: commit.subject,
          lockfilePath,
          workspacePath: getWorkspaceDisplayPath(workspacePath),
          section,
          dependency,
          change: specBefore === undefined ? 'added' : 'updated',
          specBefore,
          specAfter,
          resolvedBefore,
          resolvedAfter,
        });
      }
    }
  }

  return changes;
}

function sanitizeTsv(value: string | undefined): string {
  return (value ?? '').replaceAll('\t', ' ').replaceAll('\n', ' ');
}

function createDedupedChangeKey(change: DependencyChange): string {
  const key: DedupedChangeKey = {
    sha: change.sha,
    lockfilePath: change.lockfilePath,
    dependency: change.dependency,
    change: change.change,
    specBefore: change.specBefore ?? '',
    specAfter: change.specAfter,
    resolvedBefore: change.resolvedBefore ?? '',
    resolvedAfter: change.resolvedAfter ?? '',
  };

  return JSON.stringify(key);
}

function dedupeChanges(changes: DependencyChange[]): DependencyChange[] {
  const deduped = new Map<string, DependencyChange>();

  for (const change of changes) {
    const key = createDedupedChangeKey(change);
    if (!deduped.has(key)) {
      deduped.set(key, change);
    }
  }

  return [...deduped.values()];
}

function parseResolvedVersion(value: string | undefined): string | undefined {
  if (!value) {
    return undefined;
  }

  if (
    value.startsWith('link:') ||
    value.startsWith('workspace:') ||
    value.startsWith('file:') ||
    value.startsWith('git+') ||
    value.startsWith('github:') ||
    value.startsWith('http://') ||
    value.startsWith('https://')
  ) {
    return undefined;
  }

  return SEMVER_VERSION_REGEX.test(value)
    ? value.replace(LEADING_V_REGEX, '')
    : undefined;
}

function packageVersionKey(packageVersion: PackageVersion): string {
  return `${packageVersion.dependency}@${packageVersion.version}`;
}

function collectPackageVersions(changes: DependencyChange[]): PackageVersion[] {
  const versions = new Map<string, PackageVersion>();

  for (const change of changes) {
    const beforeVersion = parseResolvedVersion(change.resolvedBefore);
    if (beforeVersion) {
      const packageVersion = {
        dependency: change.dependency,
        version: beforeVersion,
      };
      versions.set(packageVersionKey(packageVersion), packageVersion);
    }

    const afterVersion = parseResolvedVersion(change.resolvedAfter);
    if (afterVersion) {
      const packageVersion = {
        dependency: change.dependency,
        version: afterVersion,
      };
      versions.set(packageVersionKey(packageVersion), packageVersion);
    }
  }

  return [...versions.values()];
}

function createVulnerabilityLinks(vulnerability: OsvVulnerability): string[] {
  const links: string[] = [];
  const advisoryIds = new Set<string>([
    vulnerability.id,
    ...(vulnerability.aliases ?? []),
  ]);

  for (const advisoryId of advisoryIds) {
    if (advisoryId.startsWith('GHSA-')) {
      links.push(`${GITHUB_ADVISORY_BASE_URL}/${advisoryId}`);
      continue;
    }

    if (advisoryId.startsWith('CVE-')) {
      links.push(`${NVD_CVE_BASE_URL}/${advisoryId}`);
    }
  }

  return links;
}

function dependencyVersionKey(dependency: string, version: string): string {
  return `${dependency}@${version}`;
}

function createCommitOrder(commits: Commit[]): Map<string, number> {
  const order = new Map<string, number>();
  const oldestToNewest = commits.toReversed();
  for (const [index, commit] of oldestToNewest.entries()) {
    order.set(commit.sha, index);
  }
  return order;
}

function updatePresenceRangeStart(
  accumulator: PresenceRangeAccumulator,
  order: number,
  date: string,
  sha: string,
): void {
  if (accumulator.startOrder === undefined || order < accumulator.startOrder) {
    accumulator.startOrder = order;
    accumulator.startDate = date;
    accumulator.startSha = sha;
  }
}

function updatePresenceRangeAfter(
  accumulator: PresenceRangeAccumulator,
  order: number,
  date: string,
  sha: string,
): void {
  if (
    accumulator.lastAfterOrder === undefined ||
    order > accumulator.lastAfterOrder
  ) {
    accumulator.lastAfterOrder = order;
    accumulator.lastAfterDate = date;
    accumulator.lastAfterSha = sha;
  }
}

function updatePresenceRangeBefore(
  accumulator: PresenceRangeAccumulator,
  order: number,
  date: string,
  sha: string,
): void {
  if (
    accumulator.lastBeforeOrder === undefined ||
    order > accumulator.lastBeforeOrder
  ) {
    accumulator.lastBeforeOrder = order;
    accumulator.lastBeforeDate = date;
    accumulator.lastBeforeSha = sha;
  }
}

function computePresenceRanges(
  changes: DependencyChange[],
  commits: Commit[],
): Map<string, PresenceRange> {
  const ranges = new Map<string, PresenceRangeAccumulator>();
  const commitOrder = createCommitOrder(commits);

  for (const change of changes) {
    const order = commitOrder.get(change.sha);
    if (order === undefined) {
      continue;
    }

    const beforeVersion = parseResolvedVersion(change.resolvedBefore);
    if (beforeVersion) {
      const key = dependencyVersionKey(change.dependency, beforeVersion);
      const accumulator = ranges.get(key) ?? {};
      updatePresenceRangeStart(accumulator, order, change.date, change.sha);
      updatePresenceRangeBefore(accumulator, order, change.date, change.sha);
      ranges.set(key, accumulator);
    }

    const afterVersion = parseResolvedVersion(change.resolvedAfter);
    if (afterVersion) {
      const key = dependencyVersionKey(change.dependency, afterVersion);
      const accumulator = ranges.get(key) ?? {};
      updatePresenceRangeStart(accumulator, order, change.date, change.sha);
      updatePresenceRangeAfter(accumulator, order, change.date, change.sha);
      ranges.set(key, accumulator);
    }
  }

  const output = new Map<string, PresenceRange>();
  for (const [key, accumulator] of ranges) {
    const startKnown = accumulator.lastAfterOrder !== undefined;
    const endKnown =
      accumulator.lastBeforeOrder !== undefined &&
      (accumulator.lastAfterOrder === undefined ||
        accumulator.lastBeforeOrder > accumulator.lastAfterOrder);
    const startDate = accumulator.startDate ?? '';
    const startSha = accumulator.startSha ?? '';
    const endDate =
      accumulator.lastBeforeDate ??
      accumulator.lastAfterDate ??
      accumulator.startDate ??
      '';
    const endSha =
      accumulator.lastBeforeSha ??
      accumulator.lastAfterSha ??
      accumulator.startSha ??
      '';

    output.set(key, {
      dateStart: startKnown ? startDate : '',
      dateEnd: endKnown ? endDate : '',
      commitStart: startKnown ? startSha : '',
      commitEnd: endKnown ? endSha : '',
      startKnown,
      endKnown,
    });
  }

  return output;
}

function hasKnownVulnerability(change: AnnotatedDependencyChange): boolean {
  return Boolean(change.knownVulnsBefore || change.knownVulnsAfter);
}

function hasKnownVulnerabilityBefore(
  change: AnnotatedDependencyChange,
): boolean {
  return Boolean(change.knownVulnsBefore);
}

function hasKnownVulnerabilityAfter(
  change: AnnotatedDependencyChange,
): boolean {
  return Boolean(change.knownVulnsAfter);
}

async function lookupVulnerabilityLinks(
  changes: DependencyChange[],
): Promise<Map<string, string>> {
  const packageVersions = collectPackageVersions(changes);
  if (packageVersions.length === 0) {
    return new Map<string, string>();
  }

  const body: OsvBatchRequest = {
    queries: packageVersions.map(packageVersion => ({
      package: {
        ecosystem: 'npm',
        name: packageVersion.dependency,
      },
      version: packageVersion.version,
    })),
  };

  const response = await fetch(OSV_BATCH_QUERY_URL, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(
      `OSV query failed (${response.status} ${response.statusText}): ${errorText}`,
    );
  }

  const data = (await response.json()) as OsvBatchResponse;
  const linksByPackageVersion = new Map<string, string>();

  for (const [index, packageVersion] of packageVersions.entries()) {
    const result = data.results[index];
    const linkSet = new Set<string>();

    for (const vulnerability of result?.vulns ?? []) {
      for (const link of createVulnerabilityLinks(vulnerability)) {
        linkSet.add(link);
      }
    }

    linksByPackageVersion.set(
      packageVersionKey(packageVersion),
      [...linkSet].toSorted().join(', '),
    );
  }

  return linksByPackageVersion;
}

async function annotateChangesWithVulnerabilities(
  changes: DependencyChange[],
  commits: Commit[],
): Promise<AnnotatedDependencyChange[]> {
  const vulnerabilityLinks = await lookupVulnerabilityLinks(changes);
  const presenceRanges = computePresenceRanges(changes, commits);

  return changes.map(change => {
    const beforeVersion = parseResolvedVersion(change.resolvedBefore);
    const afterVersion = parseResolvedVersion(change.resolvedAfter);
    const knownVulnsBefore = beforeVersion
      ? (vulnerabilityLinks.get(`${change.dependency}@${beforeVersion}`) ?? '')
      : '';
    const knownVulnsAfter = afterVersion
      ? (vulnerabilityLinks.get(`${change.dependency}@${afterVersion}`) ?? '')
      : '';

    const rangeVersion =
      (knownVulnsAfter && afterVersion) ||
      (knownVulnsBefore && beforeVersion) ||
      afterVersion ||
      beforeVersion;
    const range = rangeVersion
      ? (presenceRanges.get(
          dependencyVersionKey(change.dependency, rangeVersion),
        ) ?? {
          dateStart: '',
          dateEnd: '',
          commitStart: '',
          commitEnd: '',
          startKnown: false,
          endKnown: false,
        })
      : {
          dateStart: '',
          dateEnd: '',
          commitStart: '',
          commitEnd: '',
          startKnown: false,
          endKnown: false,
        };

    return {
      ...change,
      knownVulnsBefore,
      knownVulnsAfter,
      presentDateStart: range.dateStart,
      presentDateEnd: range.dateEnd,
      presentCommitStart: range.commitStart,
      presentCommitEnd: range.commitEnd,
    };
  });
}

function getNotionToken(): string {
  const token = process.env.NOTION_TOKEN;
  if (!token) {
    throw new Error('NOTION_TOKEN is required for Notion output.');
  }
  return token;
}

function createNotionText(content: string | undefined): NotionRichText[] {
  if (!content) {
    return [];
  }
  return [
    {
      type: 'text',
      text: {content: content.slice(0, 2000)},
    },
  ];
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function notionRequest<T>(
  path: string,
  token: string,
  options: NotionRequestOptions = {},
): Promise<T> {
  const {method = 'POST', body} = options;

  for (let attempt = 1; attempt <= 5; attempt++) {
    const requestInit: RequestInit = {
      method,
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json',
        'Notion-Version': NOTION_API_VERSION,
      },
    };
    if (body !== undefined) {
      requestInit.body = JSON.stringify(body);
    }

    const response = await fetch(`${NOTION_API_BASE_URL}${path}`, requestInit);

    if (response.ok) {
      return (await response.json()) as T;
    }

    if (response.status === 429 && attempt < 5) {
      const retryAfterSeconds = Number(
        response.headers.get('retry-after') ?? 1,
      );
      await sleep(retryAfterSeconds * 1000);
      continue;
    }

    const errorText = await response.text();
    throw new Error(
      `Notion API request failed (${response.status} ${response.statusText}): ${errorText}`,
    );
  }

  throw new Error('Notion API request failed after retries.');
}

async function createNotionDatabase(
  token: string,
  parentPageId: string,
  title: string,
): Promise<NotionDatabaseResponse> {
  return await notionRequest<NotionDatabaseResponse>('/databases', token, {
    body: {
      parent: {
        type: 'page_id',
        page_id: parentPageId,
      },
      title: createNotionText(title),
      properties: {
        'Dependency': {title: {}},
        'Change': {
          select: {
            options: [
              {name: 'added', color: 'green'},
              {name: 'updated', color: 'blue'},
            ],
          },
        },
        'Date': {date: {}},
        'Workspace': {rich_text: {}},
        'Section': {
          select: {
            options: DEPENDENCY_FIELDS.map(name => ({name, color: 'default'})),
          },
        },
        'Lockfile': {rich_text: {}},
        'Spec Before': {rich_text: {}},
        'Spec After': {rich_text: {}},
        'Resolved Before': {rich_text: {}},
        'Resolved After': {rich_text: {}},
        'Present Date Start': {rich_text: {}},
        'Present Date End': {rich_text: {}},
        'Present Commit Start': {rich_text: {}},
        'Present Commit End': {rich_text: {}},
        'Known Vulns Before': {rich_text: {}},
        'Known Vulns After': {rich_text: {}},
        'SHA': {rich_text: {}},
        'Commit': {url: {}},
        'Subject': {rich_text: {}},
      },
    },
  });
}

async function createNotionPage(
  token: string,
  databaseId: string,
  change: AnnotatedDependencyChange,
): Promise<void> {
  await notionRequest('/pages', token, {
    body: {
      parent: {
        database_id: databaseId,
      },
      properties: {
        'Dependency': {
          title: createNotionText(change.dependency),
        },
        'Change': {
          select: {name: change.change},
        },
        'Date': {
          date: {start: change.date},
        },
        'Workspace': {
          rich_text: createNotionText(change.workspacePath),
        },
        'Section': {
          select: {name: change.section},
        },
        'Lockfile': {
          rich_text: createNotionText(change.lockfilePath),
        },
        'Spec Before': {
          rich_text: createNotionText(change.specBefore),
        },
        'Spec After': {
          rich_text: createNotionText(change.specAfter),
        },
        'Resolved Before': {
          rich_text: createNotionText(change.resolvedBefore),
        },
        'Resolved After': {
          rich_text: createNotionText(change.resolvedAfter),
        },
        'Present Date Start': {
          rich_text: createNotionText(change.presentDateStart),
        },
        'Present Date End': {
          rich_text: createNotionText(change.presentDateEnd),
        },
        'Present Commit Start': {
          rich_text: createNotionText(change.presentCommitStart),
        },
        'Present Commit End': {
          rich_text: createNotionText(change.presentCommitEnd),
        },
        'Known Vulns Before': {
          rich_text: createNotionText(change.knownVulnsBefore),
        },
        'Known Vulns After': {
          rich_text: createNotionText(change.knownVulnsAfter),
        },
        'SHA': {
          rich_text: createNotionText(change.sha),
        },
        'Commit': {
          url: githubCommitBaseUrl
            ? `${githubCommitBaseUrl}/${change.sha}`
            : `sha:${change.sha}`,
        },
        'Subject': {
          rich_text: createNotionText(change.subject),
        },
      },
    },
  });
}

function outputTsv(
  changes: AnnotatedDependencyChange[],
  commits: Commit[],
): void {
  console.log(
    [
      'sha',
      'date',
      'lockfile',
      'workspace',
      'section',
      'dependency',
      'change',
      'spec_before',
      'spec_after',
      'resolved_before',
      'resolved_after',
      'present_date_start',
      'present_date_end',
      'present_commit_start',
      'present_commit_end',
      'known_vulns_before',
      'known_vulns_after',
      'has_known_vuln',
      'subject',
    ].join('\t'),
  );

  for (const change of changes) {
    console.log(
      [
        change.sha,
        change.date,
        change.lockfilePath,
        change.workspacePath,
        change.section,
        change.dependency,
        change.change,
        sanitizeTsv(change.specBefore),
        sanitizeTsv(change.specAfter),
        sanitizeTsv(change.resolvedBefore),
        sanitizeTsv(change.resolvedAfter),
        sanitizeTsv(change.presentDateStart),
        sanitizeTsv(change.presentDateEnd),
        sanitizeTsv(change.presentCommitStart),
        sanitizeTsv(change.presentCommitEnd),
        sanitizeTsv(change.knownVulnsBefore),
        sanitizeTsv(change.knownVulnsAfter),
        hasKnownVulnerability(change) ? 'YES' : 'NO',
        sanitizeTsv(change.subject),
      ].join('\t'),
    );
  }

  console.error(
    `Printed ${changes.length} dependency changes across ${commits.length} commits.`,
  );
}

function escapeHtml(value: string | undefined): string {
  return (value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function renderVulnerabilityLinks(urls: string): string {
  if (!urls) {
    return '';
  }

  return urls
    .split(',')
    .map(url => url.trim())
    .filter(Boolean)
    .map(url => {
      const label = url.startsWith(`${GITHUB_ADVISORY_BASE_URL}/`)
        ? url.slice(`${GITHUB_ADVISORY_BASE_URL}/`.length)
        : url.startsWith(`${NVD_CVE_BASE_URL}/`)
          ? url.slice(`${NVD_CVE_BASE_URL}/`.length)
          : url;

      return `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`;
    })
    .join('<br>');
}

function renderCommitHashLink(sha: string | undefined): string {
  if (!sha) {
    return '';
  }
  if (!githubCommitBaseUrl) {
    return escapeHtml(sha.slice(0, 12));
  }
  return `<a href="${githubCommitBaseUrl}/${escapeHtml(sha)}" target="_blank" rel="noopener noreferrer">${escapeHtml(sha.slice(0, 12))}</a>`;
}

function renderDependencyLink(dependency: string): string {
  const url = `https://npmx.dev/search?q=${encodeURIComponent(dependency)}`;
  return `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">${escapeHtml(dependency)}</a>`;
}

function getHtmlRowType(
  change: AnnotatedDependencyChange,
): 'active' | 'resolved' | 'none' {
  if (hasKnownVulnerabilityAfter(change)) {
    return 'active';
  }

  if (hasKnownVulnerabilityBefore(change)) {
    return 'resolved';
  }

  return 'none';
}

function getHtmlRowIsFixed(change: AnnotatedDependencyChange): boolean {
  const hasVulnerability = Boolean(
    change.knownVulnsBefore || change.knownVulnsAfter,
  );
  return hasVulnerability && Boolean(change.presentDateEnd);
}

function renderHtmlRow(change: AnnotatedDependencyChange): string {
  const rowClass = hasKnownVulnerabilityAfter(change)
    ? 'vuln-row'
    : hasKnownVulnerabilityBefore(change)
      ? 'vuln-resolved-row'
      : '';
  const rowType = getHtmlRowType(change);
  const isFixed = getHtmlRowIsFixed(change);
  return `
      <tr class="${rowClass}" data-row-type="${rowType}" data-is-fixed="${isFixed ? 'true' : 'false'}">
        <td>${renderCommitHashLink(change.sha)}</td>
        <td class="date-col">${escapeHtml(change.date)}</td>
        <td class="optional-col">${escapeHtml(change.lockfilePath)}</td>
        <td class="optional-col">${escapeHtml(change.workspacePath)}</td>
        <td class="optional-col">${escapeHtml(change.section)}</td>
        <td>${renderDependencyLink(change.dependency)}</td>
        <td>${escapeHtml(change.change)}</td>
        <td class="optional-col">${escapeHtml(change.specBefore)}</td>
        <td class="optional-col">${escapeHtml(change.specAfter)}</td>
        <td>${escapeHtml(change.resolvedBefore)}</td>
        <td>${escapeHtml(change.resolvedAfter)}</td>
        <td class="date-col">${escapeHtml(change.presentDateStart)}</td>
        <td class="date-col">${escapeHtml(change.presentDateEnd)}</td>
        <td>${renderCommitHashLink(change.presentCommitStart)}</td>
        <td>${renderCommitHashLink(change.presentCommitEnd)}</td>
        <td>${renderVulnerabilityLinks(change.knownVulnsBefore)}</td>
        <td>${renderVulnerabilityLinks(change.knownVulnsAfter)}</td>
        <td>${escapeHtml(change.subject)}</td>
      </tr>`;
}

function buildHtmlReport(
  changes: AnnotatedDependencyChange[],
  commits: Commit[],
  since: string,
  showAllDepsDefault: boolean,
): string {
  const generatedAt = new Date().toISOString();
  const rows = changes.map(renderHtmlRow).join('\n');

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Package Lock Dependency Changes</title>
  <style>
    :root {
      color-scheme: light;
      font-family: Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    }
    body {
      margin: 0;
      background: #f7f7fb;
      color: #20242b;
    }
    .container {
      padding: 24px;
    }
    h1 {
      margin-top: 0;
      font-size: 24px;
    }
    .meta {
      margin-bottom: 16px;
      font-size: 13px;
      line-height: 1.6;
      color: #4d5562;
    }
    .filters {
      display: flex;
      flex-wrap: wrap;
      gap: 14px;
      margin-top: 8px;
      margin-bottom: 8px;
      color: #20242b;
    }
    .filters label {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 12px;
    }
    .table-wrap {
      overflow: auto;
      border: 1px solid #d8dde8;
      border-radius: 8px;
      background: #fff;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 1400px;
      font-size: 12px;
    }
    th,
    td {
      border-bottom: 1px solid #eef1f6;
      padding: 8px;
      text-align: left;
      vertical-align: top;
    }
    th {
      position: sticky;
      top: 0;
      background: #f0f4fb;
      font-weight: 700;
      z-index: 1;
    }
    tr:nth-child(even) td {
      background: #fbfdff;
    }
    tr.vuln-row td {
      background: #fff4f1;
    }
    tr.vuln-resolved-row td {
      background: #eef9eb;
    }
    a {
      color: #0d4aa8;
      text-decoration: none;
    }
    a:hover {
      text-decoration: underline;
    }
    .optional-col {
      display: none;
    }
    #report-root.show-extra-columns .optional-col {
      display: table-cell;
    }
    .date-col {
      white-space: nowrap;
    }
  </style>
</head>
<body>
  <div class="container" id="report-root">
    <h1>Package Lock Dependency Changes</h1>
    <div class="meta">
      <div><strong>Generated:</strong> ${escapeHtml(generatedAt)}</div>
      <div><strong>Since:</strong> ${escapeHtml(since)}</div>
      <div><strong>Commits scanned:</strong> ${commits.length}</div>
      <div><strong>Visible rows:</strong> <span id="visible-row-count">${changes.length}</span> / ${changes.length}</div>
      <div class="filters">
        <label><input id="hide-fixed" type="checkbox"> Hide fixed</label>
        <label><input id="show-all-deps" type="checkbox" ${showAllDepsDefault ? 'checked' : ''}> Show all deps changes</label>
        <label><input id="show-extra-columns" type="checkbox"> Show extra columns</label>
      </div>
    </div>
    <div class="table-wrap">
      <table class="report-table">
        <thead>
          <tr>
            <th>SHA</th>
            <th>Date</th>
            <th class="optional-col">Lockfile</th>
            <th class="optional-col">Workspace</th>
            <th class="optional-col">Section</th>
            <th>Dependency</th>
            <th>Change</th>
            <th class="optional-col">Spec Before</th>
            <th class="optional-col">Spec After</th>
            <th>Resolved Before</th>
            <th>Resolved After</th>
            <th>Present Date Start</th>
            <th>Present Date End</th>
            <th>Present Commit Start</th>
            <th>Present Commit End</th>
            <th>Known Vulns Before</th>
            <th>Known Vulns After</th>
            <th>Subject</th>
          </tr>
        </thead>
        <tbody>
${rows}
        </tbody>
      </table>
    </div>
  </div>
  <script>
    (() => {
      const hideFixed = document.getElementById('hide-fixed');
      const showAllDeps = document.getElementById('show-all-deps');
      const showExtraColumns = document.getElementById('show-extra-columns');
      const visibleRowCount = document.getElementById('visible-row-count');
      const reportRoot = document.getElementById('report-root');
      const rows = Array.from(document.querySelectorAll('tbody tr'));

      const applyFilters = () => {
        let visible = 0;

        for (const row of rows) {
          const rowType = row.getAttribute('data-row-type') ?? 'none';
          const isFixed = row.getAttribute('data-is-fixed') === 'true';
          let show = true;

          if (!showAllDeps.checked && rowType === 'none') {
            show = false;
          }

          if (hideFixed.checked && isFixed) {
            show = false;
          }

          row.hidden = !show;
          if (show) {
            visible++;
          }
        }

        visibleRowCount.textContent = String(visible);
      };

      hideFixed.addEventListener('change', applyFilters);
      showAllDeps.addEventListener('change', applyFilters);
      showExtraColumns.addEventListener('change', () => {
        reportRoot.classList.toggle('show-extra-columns', showExtraColumns.checked);
      });
      applyFilters();
    })();
  </script>
</body>
</html>`;
}

async function outputHtml(
  changes: AnnotatedDependencyChange[],
  commits: Commit[],
  options: Options,
): Promise<void> {
  const html = buildHtmlReport(
    changes,
    commits,
    options.since,
    options.allDeps,
  );

  const outputPath = join(repoRoot, options.htmlFile);
  await writeFile(outputPath, html, 'utf8');
  console.log(outputPath);
  console.error(
    `Wrote HTML report with ${changes.length} dependency changes across ${commits.length} commits.`,
  );
}

function markdownLinks(urls: string): string {
  if (!urls) {
    return '';
  }

  return urls
    .split(',')
    .map(url => url.trim())
    .filter(Boolean)
    .map(url => `[${url}](${url})`)
    .join('<br>');
}

function escapeMarkdownCell(value: string | undefined): string {
  return (value ?? '').replaceAll('|', '\\|').replaceAll('\n', '<br>');
}

function buildMarkdownReport(
  changes: AnnotatedDependencyChange[],
  commits: Commit[],
  since: string,
): string {
  const generatedAt = new Date().toISOString();
  const header = [
    '# Package Lock Dependency Changes',
    '',
    `- Generated: ${generatedAt}`,
    `- Since: ${since}`,
    `- Commits scanned: ${commits.length}`,
    `- Rows: ${changes.length}`,
    '',
    '| Vuln | SHA | Date | Lockfile | Workspace | Section | Dependency | Change | Spec Before | Spec After | Resolved Before | Resolved After | Present Date Start | Present Date End | Present Commit Start | Present Commit End | Known Vulns Before | Known Vulns After | Subject |',
    '| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |',
  ];

  const rows = changes.map(change => {
    const commitUrl = githubCommitBaseUrl
      ? `${githubCommitBaseUrl}/${change.sha}`
      : undefined;
    return [
      hasKnownVulnerability(change) ? '**YES**' : '',
      commitUrl
        ? `[${change.sha.slice(0, 12)}](${commitUrl})`
        : change.sha.slice(0, 12),
      escapeMarkdownCell(change.date),
      escapeMarkdownCell(change.lockfilePath),
      escapeMarkdownCell(change.workspacePath),
      escapeMarkdownCell(change.section),
      escapeMarkdownCell(change.dependency),
      escapeMarkdownCell(change.change),
      escapeMarkdownCell(change.specBefore),
      escapeMarkdownCell(change.specAfter),
      escapeMarkdownCell(change.resolvedBefore),
      escapeMarkdownCell(change.resolvedAfter),
      escapeMarkdownCell(change.presentDateStart),
      escapeMarkdownCell(change.presentDateEnd),
      escapeMarkdownCell(change.presentCommitStart),
      escapeMarkdownCell(change.presentCommitEnd),
      markdownLinks(change.knownVulnsBefore),
      markdownLinks(change.knownVulnsAfter),
      escapeMarkdownCell(change.subject),
    ].join(' | ');
  });

  return [...header, ...rows, ''].join('\n');
}

async function outputMarkdown(
  changes: AnnotatedDependencyChange[],
  commits: Commit[],
  options: Options,
): Promise<void> {
  const markdown = buildMarkdownReport(changes, commits, options.since);

  const outputPath = join(repoRoot, options.mdFile);
  await writeFile(outputPath, markdown, 'utf8');
  console.log(outputPath);
  console.error(
    `Wrote Markdown report with ${changes.length} dependency changes across ${commits.length} commits.`,
  );
}

async function outputJson(
  changes: AnnotatedDependencyChange[],
  commits: Commit[],
  options: Options,
): Promise<void> {
  const payload = {
    generatedAt: new Date().toISOString(),
    since: options.since,
    summary: {
      commitsScanned: commits.length,
      rows: changes.length,
    },
    changes: changes.map(change => ({
      ...change,
      hasKnownVuln: hasKnownVulnerability(change),
    })),
  };

  const outputPath = join(repoRoot, options.jsonFile);
  await writeFile(outputPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
  console.log(outputPath);
  console.error(
    `Wrote JSON report with ${changes.length} dependency changes across ${commits.length} commits.`,
  );
}

async function outputNotion(
  changes: AnnotatedDependencyChange[],
  commits: Commit[],
  options: Options,
): Promise<void> {
  if (!options.notionParentPageId) {
    throw new Error('--notion-parent-page-id is required for Notion output.');
  }

  const token = getNotionToken();
  const database = await createNotionDatabase(
    token,
    options.notionParentPageId,
    options.notionDatabaseTitle,
  );

  console.error(`Created Notion database: ${database.url}`);

  for (const [index, change] of changes.entries()) {
    await createNotionPage(token, database.id, change);

    if ((index + 1) % 25 === 0 || index + 1 === changes.length) {
      console.error(`Inserted ${index + 1}/${changes.length} rows into Notion`);
    }
  }

  console.log(database.url);
  console.error(
    `Created Notion database with ${changes.length} dependency changes across ${commits.length} commits.`,
  );
}

async function main(): Promise<void> {
  const commits = await getCommits(options.since, options.lockfiles);
  const rawChanges: DependencyChange[] = [];

  for (const commit of commits) {
    const touchedLockfiles = await getTouchedLockfiles(
      commit.sha,
      options.lockfiles,
    );

    for (const lockfilePath of touchedLockfiles) {
      const [beforeContent, afterContent] = await Promise.all([
        getFileAtRevision(`${commit.sha}^`, lockfilePath),
        getFileAtRevision(commit.sha, lockfilePath),
      ]);
      const beforeLockfile = parseLockfile(
        beforeContent,
        `${commit.sha}^`,
        lockfilePath,
      );
      const afterLockfile = parseLockfile(
        afterContent,
        commit.sha,
        lockfilePath,
      );
      const lockfileChanges = collectDependencyChanges(
        commit,
        lockfilePath,
        beforeLockfile,
        afterLockfile,
      );

      rawChanges.push(...lockfileChanges);
    }
  }

  const changes = dedupeChanges(rawChanges);
  const annotatedChanges = await annotateChangesWithVulnerabilities(
    changes,
    commits,
  );
  const visibleChanges = options.allDeps
    ? annotatedChanges
    : annotatedChanges.filter(hasKnownVulnerability);

  if (options.output === 'notion') {
    await outputNotion(visibleChanges, commits, options);
    return;
  }

  if (options.output === 'html') {
    await outputHtml(annotatedChanges, commits, options);
    return;
  }

  if (options.output === 'md') {
    await outputMarkdown(visibleChanges, commits, options);
    return;
  }

  if (options.output === 'json') {
    await outputJson(visibleChanges, commits, options);
    return;
  }

  outputTsv(visibleChanges, commits);
}

try {
  await main();
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  console.error(message);
  process.exit(1);
}
