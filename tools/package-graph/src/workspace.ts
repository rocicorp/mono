/**
 * The zero monorepo's view of the workspace model (`pnpm graph`).
 *
 * pnpm owns package membership (`pnpm -r ls`, expanding pnpm-workspace.yaml);
 * each member's package.json owns its dependency edges. The LAYERS table below
 * is the ONE hand-curated input, and the validation in `buildModel` makes a new
 * or renamed workspace member fail loudly until it is placed. Everything else is
 * derived by the shared core in `model.ts`.
 *
 * Unlike a typical JS workspace, devDependencies are INCLUDED. Packages here are
 * consumed as TypeScript source over relative paths
 * (`../../zero-protocol/src/ast.ts`), so an internal package almost always
 * appears under `devDependencies` -- 120 of the 136 internal edges do. Dropping
 * them, as a published-artifact view would, leaves an empty graph. The edge's
 * `kind` records which section declared it, and the interactive view can filter
 * on it. `tools/verify-package-deps` is what keeps these manifests honest
 * against the actual imports.
 */

import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {join, resolve} from 'node:path';
import {extractMetrics} from './metrics-catalog.ts';
import {
  buildModel,
  REPO_ROOT,
  repoRelative,
  type EdgeKind,
  type Layer,
  type Meta,
  type Model,
  type SourceEdge,
  type SourcePackage,
  type TargetKind,
} from './model.ts';

/**
 * Ordered from lowest-level dependencies to highest-level consumers, following
 * the hierarchy documented in AGENTS.md. An edge that points UP this list is
 * reported as a layer inversion -- that report is the point of this tool.
 *
 * The interactive view's palette carries six layer colors, so six is also the
 * ceiling.
 */
export const LAYERS: readonly Layer[] = [
  {
    // Pure types, wire formats, and utilities. Nothing here knows what a query
    // is.
    id: 'foundations',
    label: 'Foundations',
    packages: [
      '@rocicorp/zero-events',
      'datadog',
      'otel',
      'shared',
      'zero-protocol',
      'zero-types',
    ],
  },
  {
    // The IVM engine, the schema/permission language, and the compilers that
    // translate a ZQL AST into something else.
    id: 'query_engine',
    label: 'Query engine & schema',
    packages: ['ast-to-zql', 'z2s', 'zero-permissions', 'zero-schema', 'zql'],
  },
  {
    // Where rows actually live: the client-side sync store, the SQLite replica,
    // and the server that keeps them in step with Postgres.
    id: 'storage',
    label: 'Storage & replication',
    packages: ['replicache', 'zero-cache', 'zqlite'],
  },
  {
    id: 'sdks',
    label: 'Client & server SDKs',
    packages: ['zero-client', 'zero-pg', 'zero-server'],
  },
  {
    id: 'bindings',
    label: 'Framework bindings & packaging',
    packages: [
      '@rocicorp/zero',
      'analyze-query',
      'zero-react',
      'zero-react-native',
      'zero-solid',
    ],
  },
  {
    // Everything nothing else imports: deployables, dev tooling, benchmarks and
    // integration suites.
    id: 'apps_tools',
    label: 'Apps, tools & harnesses',
    packages: [
      'client-simulator',
      'load-generator',
      'otel-proxy',
      'package-graph',
      'process-tracker',
      'replicache-doc',
      'replicache-perf',
      'scripts',
      'sqlite-io-yield-simulator',
      'verify-package-deps',
      'zbugs',
      'zero-sst',
      'zero-throughput',
      'zql-benchmarks',
      'zql-integration-tests',
      'zql-viz',
      'zqlite-zql-test',
    ],
  },
];

/** What the renderers need to name this workspace; travels as `model.meta`. */
export const META: Meta = {
  title: 'Zero workspace',
  heading: 'Package dependency map',
  command: 'graph',
  generator: 'tools/package-graph/src/main.ts',
  layersFile: 'tools/package-graph/src/workspace.ts',
  membershipSource: 'pnpm workspace membership',
  declaredIn: 'package.json',
  externals:
    'External npm packages are omitted; internal devDependencies are not.',
  docPath: 'docs/PACKAGE-GRAPH.md',
  metricsPath: 'docs/METRICS.md',
  modelPath: 'docs/graph/model.json',
  htmlPath: 'docs/graph/index.html',
};

type Manifest = {
  name?: string | undefined;
  version?: string | undefined;
  description?: string | undefined;
  private?: boolean | undefined;
  dependencies?: Record<string, string> | undefined;
  devDependencies?: Record<string, string> | undefined;
  optionalDependencies?: Record<string, string> | undefined;
  peerDependencies?: Record<string, string> | undefined;
  peerDependenciesMeta?:
    | Record<string, {optional?: boolean | undefined}>
    | undefined;
};

type Member = {name: string; path: string};

export function pnpmMembers(): Member[] {
  let stdout: string;
  try {
    stdout = execFileSync('pnpm', ['-r', 'ls', '--depth', '-1', '--json'], {
      cwd: REPO_ROOT,
      encoding: 'utf8',
      maxBuffer: 64 * 1024 * 1024,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
  } catch (error) {
    const err = error as {stderr?: Buffer; message: string};
    const detail = err.stderr?.toString().trim() || err.message;
    throw new Error(`pnpm -r ls failed: ${detail}`);
  }
  // The repo-root package is the workspace harness (scripts, no exports), not a
  // member of the architecture.
  return (JSON.parse(stdout) as Member[]).filter(
    member => resolve(member.path) !== REPO_ROOT,
  );
}

/**
 * package.json has no target table, and nearly every member here is
 * `private: true` with no `exports` -- entry points would classify the whole
 * workspace as one thing. Location plus publish status is the real distinction:
 * what ships to npm, what is an internal source package, and what is only ever
 * run.
 */
function targetKind(manifest: Manifest, dir: string): TargetKind {
  if (dir.startsWith('apps/')) return 'app';
  if (dir.startsWith('tools/') || dir === 'scripts') return 'tool';
  if (dir.startsWith('prod/')) return 'deployment';
  return manifest.private === true ? 'internal package' : 'published package';
}

/**
 * `optional` renders dashed and `kind` renders as a qualifier. A package listed
 * in two sections keeps the strongest claim: runtime beats dev, required beats
 * optional.
 */
const DEPENDENCY_SECTIONS: readonly (readonly [
  keyof Manifest,
  (manifest: Manifest, name: string) => {kind: EdgeKind; optional: boolean},
])[] = [
  ['dependencies', () => ({kind: 'runtime', optional: false})],
  ['optionalDependencies', () => ({kind: 'runtime', optional: true})],
  ['devDependencies', () => ({kind: 'dev', optional: false})],
  [
    'peerDependencies',
    (manifest, name) => ({
      kind: 'peer',
      optional: Boolean(manifest.peerDependenciesMeta?.[name]?.optional),
    }),
  ],
];

const KIND_RANK: Record<EdgeKind, number> = {runtime: 0, peer: 1, dev: 2};

export function workspaceModel(
  members: Member[] = pnpmMembers(),
  withMetrics = true,
): Model {
  const manifests = members.map(member => ({
    dir: repoRelative(member.path),
    manifest: JSON.parse(
      readFileSync(join(member.path, 'package.json'), 'utf8'),
    ) as Manifest,
  }));
  const packageNames = new Set(manifests.map(({manifest}) => manifest.name));

  const packages: SourcePackage[] = manifests.map(({dir, manifest}) => ({
    name: manifest.name!,
    targetKind: targetKind(manifest, dir),
    version: manifest.version ?? '0.0.0',
    description: manifest.description || null,
    dir,
    manifestPath: `${dir}/package.json`,
  }));

  const edges: SourceEdge[] = [];
  for (const {manifest} of manifests) {
    const byDependency = new Map<string, SourceEdge>();
    for (const [section, classify] of DEPENDENCY_SECTIONS) {
      const declared = manifest[section] as Record<string, string> | undefined;
      for (const name of Object.keys(declared ?? {})) {
        if (!packageNames.has(name) || name === manifest.name) continue;
        const {kind, optional} = classify(manifest, name);
        const previous = byDependency.get(name);
        byDependency.set(name, {
          from: manifest.name!,
          to: name,
          optional: previous ? previous.optional && optional : optional,
          kind:
            previous && KIND_RANK[previous.kind] <= KIND_RANK[kind]
              ? previous.kind
              : kind,
        });
      }
    }
    edges.push(...byDependency.values());
  }

  // The metrics overlay is not optional-by-nature: it is derived from committed
  // source, it is deterministic, and it costs one parse of the files that
  // mention an instrument. So it is folded in before anything is rendered, and
  // the committed views can carry it.
  const metrics = withMetrics ? extractMetrics(packages.map(p => p.dir)) : null;

  return buildModel({meta: META, layers: LAYERS, packages, edges, metrics});
}
