/**
 * The workspace model behind `pnpm graph`.
 *
 * `workspace.ts` owns everything specific to this repo: it asks pnpm for
 * membership, reads the dependency edges out of each package.json, and carries
 * the ONE hand-curated input (the LAYERS table). Everything here -- validation,
 * the graph math, the derived metrics -- is generic, and the validation makes a
 * new or renamed workspace member fail loudly until it is placed in LAYERS.
 *
 * Renderers (`render-markdown.ts`, `render-html.ts`) consume the built model and
 * never invoke pnpm themselves, so a new view of the workspace is a new renderer
 * rather than another traversal.
 *
 * The model is deterministic -- no timestamps, no absolute paths, stable sort
 * order -- so identical manifests produce identical bytes on every machine.
 *
 * Ported from rindle's `scripts/graph/model.mjs`.
 */

import {dirname, relative, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

export const REPO_ROOT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '..',
  '..',
  '..',
);

/**
 * Bump when the emitted JSON changes shape, so a consumer can reject a stale
 * file it cannot read.
 */
export const MODEL_SCHEMA = 1;

/** How a workspace member is meant to be consumed. */
export type TargetKind =
  | 'published package'
  | 'internal package'
  | 'app'
  | 'tool'
  | 'deployment';

/**
 * Which package.json section declared an edge. In this repo almost every
 * internal edge is a devDependency -- packages are consumed as TypeScript source
 * over relative paths, so only the published packages carry runtime deps -- and
 * dropping them would leave an empty graph, so they are first-class here.
 */
export type EdgeKind = 'runtime' | 'dev' | 'peer';

export type Layer = {
  readonly id: string;
  readonly label: string;
  readonly packages: readonly string[];
};

export type SourcePackage = {
  readonly name: string;
  readonly targetKind: TargetKind;
  readonly version: string;
  readonly description: string | null;
  readonly dir: string;
  readonly manifestPath: string;
};

export type SourceEdge = {
  readonly from: string;
  readonly to: string;
  readonly optional: boolean;
  readonly kind: EdgeKind;
};

export type Edge = SourceEdge & {
  /**
   * False when another path already communicates the relationship, so a
   * renderer can draw the legible skeleton or the full truth without
   * recomputing anything.
   */
  readonly structural: boolean;
};

export type PackageMetrics = {
  readonly fanIn: number;
  readonly fanOut: number;
  readonly cone: number;
  readonly blastRadius: number;
  readonly depth: number;
  readonly instability: number;
};

export type ModelPackage = SourcePackage & {
  readonly layer: string;
  readonly dependsOn: readonly string[];
  readonly dependents: readonly string[];
  readonly metrics: PackageMetrics;
};

export type LayerEdge = {
  readonly from: string;
  readonly to: string;
  readonly count: number;
};

export type LayerViolation = {
  readonly from: string;
  readonly to: string;
};

/** Renderer-facing identity of the workspace; travels inside `model.meta`. */
export type Meta = {
  readonly title: string;
  readonly heading: string;
  readonly command: string;
  readonly generator: string;
  readonly layersFile: string;
  readonly membershipSource: string;
  readonly declaredIn: string;
  readonly externals: string;
  readonly docPath: string;
  readonly modelPath: string;
  readonly htmlPath: string;
};

export type Model = {
  readonly schema: number;
  readonly generator: string;
  readonly meta: Meta;
  readonly layers: readonly Layer[];
  readonly packages: readonly ModelPackage[];
  readonly edges: readonly Edge[];
  readonly layerEdges: readonly LayerEdge[];
  readonly layerViolations: readonly LayerViolation[];
  readonly totals: {
    readonly packages: number;
    readonly edges: number;
    readonly structuralEdges: number;
    readonly runtimeEdges: number;
  };
};

export type ModelSource = {
  readonly meta: Meta;
  readonly layers: readonly Layer[];
  readonly packages: readonly SourcePackage[];
  readonly edges: readonly SourceEdge[];
};

export function compareText(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Absolute paths from pnpm would pin the model to one machine; everything the
 * model emits is relative to the repo root and POSIX-separated.
 */
export function repoRelative(absolute: string): string {
  return relative(REPO_ROOT, absolute).replaceAll('\\', '/');
}

function adjacencyFor(
  edges: readonly {from: string; to: string}[],
): Map<string, Set<string>> {
  const adjacency = new Map<string, Set<string>>();
  for (const {from, to} of edges) {
    let set = adjacency.get(from);
    if (!set) {
      set = new Set();
      adjacency.set(from, set);
    }
    set.add(to);
  }
  return adjacency;
}

function assertAcyclic(
  packageNames: readonly string[],
  edges: readonly SourceEdge[],
): void {
  const adjacency = adjacencyFor(edges);
  const visiting = new Set<string>();
  const visited = new Set<string>();

  function visit(name: string, path: readonly string[]): void {
    if (visiting.has(name)) {
      const start = path.indexOf(name);
      throw new Error(
        `workspace dependency cycle: ${[...path.slice(start), name].join(' -> ')}`,
      );
    }
    if (visited.has(name)) return;

    visiting.add(name);
    for (const dependency of adjacency.get(name) ?? []) {
      visit(dependency, [...path, name]);
    }
    visiting.delete(name);
    visited.add(name);
  }

  for (const name of packageNames) visit(name, []);
}

/**
 * A -> C is not structural when another A -> ... -> C path already communicates
 * the relationship. The direct edge stays in the model; only its `structural`
 * flag goes false.
 */
function structuralEdgeSet(edges: readonly SourceEdge[]): Set<string> {
  const adjacency = adjacencyFor(edges);

  function hasAlternatePath(from: string, target: string): boolean {
    const seen = new Set([from]);
    const stack = [...(adjacency.get(from) ?? [])].filter(
      name => name !== target,
    );

    while (stack.length) {
      const current = stack.pop()!;
      if (current === target) return true;
      if (seen.has(current)) continue;
      seen.add(current);
      stack.push(...(adjacency.get(current) ?? []));
    }
    return false;
  }

  const structural = new Set<string>();
  for (const {from, to} of edges) {
    if (!hasAlternatePath(from, to)) structural.add(`${from}\0${to}`);
  }
  return structural;
}

/** Everything reachable from `start`, excluding `start` itself. */
function closure(
  adjacency: Map<string, Set<string>>,
  start: string,
): Set<string> {
  const seen = new Set<string>();
  const stack = [...(adjacency.get(start) ?? [])];
  while (stack.length) {
    const current = stack.pop()!;
    if (seen.has(current)) continue;
    seen.add(current);
    stack.push(...(adjacency.get(current) ?? []));
  }
  return seen;
}

/** Longest path down to a leaf -- the worst-case dependency depth. */
function depthsFor(
  names: readonly string[],
  adjacency: Map<string, Set<string>>,
): Map<string, number> {
  const depths = new Map<string, number>();

  function visit(name: string): number {
    const cached = depths.get(name);
    if (cached !== undefined) return cached;
    let depth = 0;
    for (const dependency of adjacency.get(name) ?? []) {
      depth = Math.max(depth, visit(dependency) + 1);
    }
    depths.set(name, depth);
    return depth;
  }

  for (const name of names) visit(name);
  return depths;
}

export function buildModel(source: ModelSource): Model {
  const {meta, layers} = source;
  const packages = source.packages.toSorted((a, b) =>
    compareText(a.name, b.name),
  );
  const packageNames = new Set(packages.map(pkg => pkg.name));
  if (packageNames.size !== packages.length) {
    throw new Error('workspace packages do not have unique names');
  }

  const packageToLayer = new Map<string, string>();
  for (const layer of layers) {
    for (const name of layer.packages) {
      if (packageToLayer.has(name)) {
        throw new Error(
          `${name} is assigned to more than one architectural layer`,
        );
      }
      packageToLayer.set(name, layer.id);
    }
  }

  const stale = [...packageToLayer.keys()]
    .filter(name => !packageNames.has(name))
    .sort(compareText);
  const missing = [...packageNames]
    .filter(name => !packageToLayer.has(name))
    .sort(compareText);
  if (stale.length || missing.length) {
    const details: string[] = [];
    if (missing.length) {
      details.push(`unplaced workspace packages: ${missing.join(', ')}`);
    }
    if (stale.length) {
      details.push(
        `layer entries that are no longer workspace packages: ${stale.join(', ')}`,
      );
    }
    throw new Error(
      `${details.join('; ')}. Update LAYERS in ${meta.layersFile}.`,
    );
  }

  const sourceEdges = source.edges.toSorted(
    (a, b) => compareText(a.from, b.from) || compareText(a.to, b.to),
  );

  const names = packages.map(pkg => pkg.name);
  assertAcyclic(names, sourceEdges);

  const structural = structuralEdgeSet(sourceEdges);
  const edges: Edge[] = sourceEdges.map(edge => ({
    ...edge,
    structural: structural.has(`${edge.from}\0${edge.to}`),
  }));

  const dependsOn = adjacencyFor(edges);
  const dependents = adjacencyFor(
    edges.map(({from, to}) => ({from: to, to: from})),
  );
  const depths = depthsFor(names, dependsOn);

  const modelPackages: ModelPackage[] = packages.map(pkg => {
    const name = pkg.name;
    const fanOut = dependsOn.get(name)?.size ?? 0;
    const fanIn = dependents.get(name)?.size ?? 0;
    return {
      ...pkg,
      layer: packageToLayer.get(name)!,
      dependsOn: [...(dependsOn.get(name) ?? [])].toSorted(compareText),
      dependents: [...(dependents.get(name) ?? [])].toSorted(compareText),
      metrics: {
        fanIn,
        fanOut,
        // Transitive: everything this package pulls in, and everything that
        // breaks if it changes.
        cone: closure(dependsOn, name).size,
        blastRadius: closure(dependents, name).size,
        depth: depths.get(name) ?? 0,
        // Martin's instability: 0 = maximally depended-upon, 1 = depends on
        // everything.
        instability:
          fanIn + fanOut === 0
            ? 0
            : Number((fanOut / (fanIn + fanOut)).toFixed(3)),
      },
    };
  });

  const layerIndex = new Map(layers.map((layer, index) => [layer.id, index]));
  const crossLayerCounts = new Map<string, number>();
  for (const edge of edges) {
    const from = packageToLayer.get(edge.from)!;
    const to = packageToLayer.get(edge.to)!;
    if (from === to) continue;
    const key = `${from}\0${to}`;
    crossLayerCounts.set(key, (crossLayerCounts.get(key) ?? 0) + 1);
  }
  const layerEdges: LayerEdge[] = Array.from(
    crossLayerCounts.entries(),
    ([key, count]) => {
      const [from, to] = key.split('\0');
      return {from, to, count};
    },
  ).sort(
    (a, b) =>
      layerIndex.get(a.from)! - layerIndex.get(b.from)! ||
      layerIndex.get(a.to)! - layerIndex.get(b.to)!,
  );

  // Layers are ordered low-level first and edges point consumer -> dependency,
  // so a dependency that climbs to a HIGHER layer inverts the architecture.
  // Reported, not fatal: the check that must stay fatal is the cycle check.
  const layerViolations: LayerViolation[] = edges
    .filter(
      edge =>
        layerIndex.get(packageToLayer.get(edge.from)!)! <
        layerIndex.get(packageToLayer.get(edge.to)!)!,
    )
    .map(({from, to}) => ({from, to}));

  return {
    schema: MODEL_SCHEMA,
    generator: meta.generator,
    meta,
    layers: layers.map(layer => ({
      id: layer.id,
      label: layer.label,
      packages: layer.packages,
    })),
    packages: modelPackages,
    edges,
    layerEdges,
    layerViolations,
    totals: {
      packages: modelPackages.length,
      edges: edges.length,
      structuralEdges: edges.filter(edge => edge.structural).length,
      runtimeEdges: edges.filter(edge => edge.kind === 'runtime').length,
    },
  };
}

export function structuralEdges(model: Model): readonly Edge[] {
  return model.edges.filter(edge => edge.structural);
}

export function serializeModel(model: Model): string {
  return `${JSON.stringify(model, null, 2)}\n`;
}
