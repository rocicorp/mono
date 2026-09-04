/**
 * Markdown + Mermaid renderer for the workspace model.
 *
 * This is the COMMITTED view (`docs/PACKAGE-GRAPH.md`) -- the one that diffs in
 * review and that `--check` gates. It is deliberately the plainest renderer: no
 * interactivity, no derived metrics, just the structure pnpm and the manifests
 * declared, plus the layer inversions that structure implies. The interactive
 * view is `render-html.ts`.
 *
 * Ported from rindle's `scripts/graph/render-markdown.mjs`.
 */

import {dirname, relative, resolve} from 'node:path';
import {
  REPO_ROOT,
  structuralEdges,
  type Model,
  type ModelPackage,
} from './model.ts';

function mermaidId(prefix: string, value: string): string {
  return `${prefix}_${value.replace(/[^A-Za-z0-9_]/g, '_')}`;
}

function mermaidLabel(value: string): string {
  return value.replaceAll('&', '&amp;').replaceAll('"', '&quot;');
}

function layerOverview(model: Model): string {
  const lines = ['flowchart BT'];
  for (const layer of model.layers) {
    const count = model.packages.filter(pkg => pkg.layer === layer.id).length;
    lines.push(
      `  ${mermaidId('layer', layer.id)}["${mermaidLabel(layer.label)}<br/>${count} ${
        count === 1 ? 'package' : 'packages'
      }"]`,
    );
  }
  lines.push('');
  for (const edge of model.layerEdges) {
    lines.push(
      `  ${mermaidId('layer', edge.from)} -->|${edge.count}| ${mermaidId('layer', edge.to)}`,
    );
  }
  return lines.join('\n');
}

function packageGraph(model: Model): string {
  const packagesByName = new Map(model.packages.map(pkg => [pkg.name, pkg]));
  const lines = ['flowchart BT'];

  for (const layer of model.layers) {
    lines.push(
      `  subgraph ${mermaidId('layer', layer.id)}["${mermaidLabel(layer.label)}"]`,
    );
    for (const name of layer.packages) {
      if (!packagesByName.has(name)) continue;
      lines.push(`    ${mermaidId('pkg', name)}["${mermaidLabel(name)}"]`);
    }
    lines.push('  end', '');
  }

  for (const edge of structuralEdges(model)) {
    const arrow = edge.optional ? '-.->' : '-->';
    lines.push(
      `  ${mermaidId('pkg', edge.from)} ${arrow} ${mermaidId('pkg', edge.to)}`,
    );
  }
  return lines.join('\n');
}

function packageLink(pkg: ModelPackage, outputPath: string): string {
  let path = relative(
    dirname(outputPath),
    resolve(REPO_ROOT, pkg.dir),
  ).replaceAll('\\', '/');
  if (!path.startsWith('.')) path = `./${path}`;
  return `[\`${pkg.name}\`](${path})`;
}

function dependencyList(packageName: string, model: Model): string {
  const dependencies = model.edges.filter(edge => edge.from === packageName);
  if (!dependencies.length) return '—';
  return dependencies
    .map(edge => {
      const qualifiers = [
        edge.optional ? 'optional' : null,
        edge.kind !== 'runtime' ? edge.kind : null,
      ]
        .filter(Boolean)
        .join(', ');
      return `\`${edge.to}\`${qualifiers ? ` _(${qualifiers})_` : ''}`;
    })
    .join(', ');
}

/**
 * The reason this file exists. Layers run low to high, edges point consumer ->
 * dependency, so an edge that climbs is a package reaching into something that
 * is supposed to sit above it. Not fatal -- some of these are deliberate -- but
 * every one of them should be a decision somebody made on purpose, and putting
 * them in the committed file makes a new one show up in review.
 */
function inversionSection(model: Model): string {
  const layerLabels = new Map(model.layers.map(l => [l.id, l.label]));
  const packageLayer = new Map(model.packages.map(p => [p.name, p.layer]));

  if (!model.layerViolations.length) {
    return `
## Layer inversions

None. Every internal dependency points down the layer stack.
`;
  }

  const rows = model.layerViolations.map(({from, to}) => {
    const edge = model.edges.find(e => e.from === from && e.to === to)!;
    return `| \`${from}\` | ${layerLabels.get(packageLayer.get(from)!)} | \`${to}\` | ${layerLabels.get(
      packageLayer.get(to)!,
    )} | ${edge.kind} |`;
  });

  return `
## Layer inversions

Layers run low to high and arrows point consumer → dependency, so a dependency
that climbs to a **higher** layer is a package reaching into something meant to
sit above it. These are reported, not forbidden — but each one should be a
deliberate decision, and a new row here is worth a question in review.

| Consumer | Its layer | Depends on | Which sits in | Declared as |
| --- | --- | --- | --- | --- |
${rows.join('\n')}
`;
}

export function renderMarkdown(model: Model, outputPath: string): string {
  const meta = model.meta;
  const devEdges = model.totals.edges - model.totals.runtimeEdges;
  const packagesByName = new Map(model.packages.map(pkg => [pkg.name, pkg]));
  const rows: string[] = [];
  for (const layer of model.layers) {
    for (const name of layer.packages) {
      const pkg = packagesByName.get(name)!;
      rows.push(
        `| ${layer.label} | ${packageLink(pkg, outputPath)} | ${pkg.targetKind} | ${dependencyList(
          name,
          model,
        )} |`,
      );
    }
  }

  return `<!-- Generated by \`pnpm ${meta.command}\`; do not edit by hand. -->

# ${meta.heading}

This view comes from ${meta.membershipSource} plus the curated architectural
layers in \`${meta.layersFile}\`. Arrows point from a consumer to the internal
package it depends on. ${meta.externals}

Packages in this repo import each other as TypeScript source over relative paths,
so an internal dependency is usually declared as a **devDependency** — ${devEdges}
of the ${model.totals.edges} edges below. Those are the architecture here, not
test scaffolding, so they are drawn like any other edge;
\`tools/verify-package-deps\` is what keeps the manifests honest against the
actual imports.

**${model.totals.packages} workspace packages · ${model.totals.edges} direct internal dependencies · ${model.totals.structuralEdges} structural edges**

## Bird's-eye view

The number on an arrow is the count of direct package dependencies crossing
those two layers.

\`\`\`mermaid
${layerOverview(model)}
\`\`\`
${inversionSection(model)}
## Package paths

This diagram uses the graph's transitive reduction so the architectural paths
remain legible. For example, when \`A → B → C\` exists, a direct \`A → C\`
shortcut is left out here. The inventory below retains every direct dependency
declared in ${meta.declaredIn}.

\`\`\`mermaid
${packageGraph(model)}
\`\`\`

## Direct dependency inventory

| Layer | Package | Kind | Direct workspace dependencies |
| --- | --- | --- | --- |
${rows.join('\n')}

## Regenerate

From the repository root:

\`\`\`sh
pnpm ${meta.command}          # this file, plus the gitignored model + interactive view
pnpm ${meta.command}:check    # fail if this file is stale
pnpm ${meta.command}:open     # regenerate and open the interactive view
\`\`\`

\`pnpm ${meta.command}\` also writes two build products that are **not**
committed: \`${meta.modelPath}\` (the extracted workspace model — every renderer,
script, and agent query should read this rather than re-walking pnpm) and
\`${meta.htmlPath}\` (an interactive view of the same model, with per-package
metrics, dependency/dependent cones, and layer filtering).
`;
}
