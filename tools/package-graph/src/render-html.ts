/**
 * Interactive renderer for the workspace model: one self-contained HTML file,
 * no build step and no network, so it opens over `file://`.
 *
 * The graph is laid out in bands -- one per curated layer, foundations at the
 * bottom -- so an edge that runs DOWNWARD always means "depends on", and an edge
 * that runs upward is an inversion. Those are painted in the warning colour
 * whatever else is on screen, because finding them is the point of the tool.
 *
 * Ported from rindle's `scripts/graph/render-html.mjs`, minus the Rust-only
 * coverage and metrics-catalog overlays.
 */

import type {Model} from './model.ts';

/**
 * Colours come from the validated data-viz reference palette. Layers use
 * categorical slots 1-6 in fixed order (validated on the adjacent pairlist, both
 * modes); edge direction uses slots 7-8 (violet/red), which no layer claims, so
 * an edge colour never impersonates a layer. Identity is never carried by colour
 * alone -- every node is directly labelled and every layer band is titled.
 */
const LAYER_COLORS = {
  light: ['#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#008300'],
  dark: ['#3987e5', '#d95926', '#199e70', '#c98500', '#d55181', '#008300'],
};

/**
 * Blue ramp, 100 -> 700. Light mode walks it forward (more = darker); dark mode
 * walks it backward from step 600 (more = brighter), so magnitude always means
 * "further from the surface".
 */
const SEQUENTIAL = [
  '#cde2fb',
  '#b7d3f6',
  '#9ec5f4',
  '#86b6ef',
  '#6da7ec',
  '#5598e7',
  '#3987e5',
  '#2a78d6',
  '#256abf',
  '#1c5cab',
  '#184f95',
  '#104281',
  '#0d366b',
];

/**
 * `short` is what the header picker shows and `label` is what the ramp caption
 * spells out. A select is only as wide as its widest option, so the glossed
 * names would set the width of the whole toolbar for a reading that is already
 * printed under the ramp the moment you choose it.
 */
const METRICS = [
  {id: 'layer', label: 'Layer', short: 'Layer'},
  {id: 'fanIn', label: 'Fan-in (direct dependents)', short: 'Fan-in'},
  {id: 'fanOut', label: 'Fan-out (direct dependencies)', short: 'Fan-out'},
  {id: 'cone', label: 'Cone (transitive dependencies)', short: 'Cone'},
  {
    id: 'blastRadius',
    label: 'Blast radius (transitive dependents)',
    short: 'Blast radius',
  },
  {id: 'depth', label: 'Depth (longest path to a leaf)', short: 'Depth'},
  {
    id: 'instability',
    label: 'Instability (fan-out / total)',
    short: 'Instability',
  },
];

/**
 * The model is inert data in a JSON script block; escaping `<` is what keeps a
 * package description from being able to close the block early.
 */
function embedJson(value: unknown): string {
  return JSON.stringify(value).replaceAll('<', '\\u003c');
}

const STYLE = `
:root {
  color-scheme: light;
  --surface: #fcfcfb;
  --plane: #f9f9f7;
  --ink: #0b0b0b;
  --ink-2: #52514e;
  --ink-muted: #898781;
  --hairline: #e1e0d9;
  --rule: #c3c2b7;
  --border: rgba(11, 11, 11, 0.10);
  --edge: #c3c2b7;
  --edge-dep: #4a3aa7;
  --edge-rev: #e34948;
  --band: rgba(11, 11, 11, 0.025);
  --raised: #f1f0ec;
  --shadow: rgba(11, 11, 11, 0.13);
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    color-scheme: dark;
    --surface: #1a1a19;
    --plane: #0d0d0d;
    --ink: #ffffff;
    --ink-2: #c3c2b7;
    --ink-muted: #898781;
    --hairline: #2c2c2a;
    --rule: #383835;
    --border: rgba(255, 255, 255, 0.10);
    --edge: #383835;
    --edge-dep: #9085e9;
    --edge-rev: #e66767;
    --band: rgba(255, 255, 255, 0.03);
    --raised: #262624;
    --shadow: rgba(0, 0, 0, 0.5);
  }
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --surface: #1a1a19;
  --plane: #0d0d0d;
  --ink: #ffffff;
  --ink-2: #c3c2b7;
  --ink-muted: #898781;
  --hairline: #2c2c2a;
  --rule: #383835;
  --border: rgba(255, 255, 255, 0.10);
  --edge: #383835;
  --edge-dep: #9085e9;
  --edge-rev: #e66767;
  --band: rgba(255, 255, 255, 0.03);
  --raised: #262624;
  --shadow: rgba(0, 0, 0, 0.5);
}

* { box-sizing: border-box; }
html, body { height: 100%; }
body {
  margin: 0;
  background: var(--plane);
  color: var(--ink);
  font: 13px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
  display: flex;
  flex-direction: column;
}

/* The toolbar is built from ONE control primitive at ONE height. Every widget is
   a 28px pill -- .field (a captioned select), .toggle, .search, .icon-btn -- so
   the row reads as a single instrument rather than as a naked label, a boxed
   select, a bare checkbox and two buttons at four different heights. */
header {
  border-bottom: 1px solid var(--hairline);
  background: var(--surface);
  padding: 8px 14px;
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
}
.brand { display: flex; align-items: baseline; gap: 9px; min-width: 0; }
h1 { font-size: 13.5px; font-weight: 650; margin: 0; letter-spacing: -0.01em; white-space: nowrap; }
.totals {
  color: var(--ink-muted);
  font-size: 11.5px;
  font-variant-numeric: tabular-nums;
  white-space: nowrap;
}
.spacer { flex: 1 1 auto; }
.tools { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }

button { font: inherit; cursor: pointer; color: var(--ink); }
select { font: inherit; color: var(--ink); }
.field, .toggle, .search, .icon-btn {
  height: 28px;
  border: 1px solid var(--rule);
  border-radius: 8px;
  background: var(--surface);
  display: inline-flex;
  align-items: center;
  flex: none;
}
.field { gap: 7px; padding: 0 4px 0 9px; }
.field .k, .legend .cap {
  font-size: 9.5px;
  font-weight: 650;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  color: var(--ink-muted);
  white-space: nowrap;
}
.field select {
  border: 0;
  background: transparent;
  padding: 0 2px 0 0;
  height: 26px;
  max-width: 15em;
}
.field select:focus, .search input:focus { outline: none; }
.field:focus-within, .search:focus-within { border-color: var(--ink); }
.field:hover, .toggle:hover, .icon-btn:hover, .search:hover { border-color: var(--ink-muted); }

.search { gap: 6px; padding: 0 9px; flex: 0 1 240px; min-width: 132px; }
.search svg { flex: none; color: var(--ink-muted); }
.search input {
  font: inherit;
  color: var(--ink);
  border: 0;
  background: transparent;
  padding: 0;
  height: 26px;
  width: 100%;
  min-width: 0;
}

/* A pressed pill, not a checkbox: at 28px it lines up with the selects either
   side of it, where a bare checkbox and its stray label were the loosest thing
   in the row. */
.toggle { gap: 6px; padding: 0 10px; color: var(--ink-muted); background: var(--surface); }
.toggle::before {
  content: "";
  width: 7px; height: 7px;
  border-radius: 50%;
  border: 1px solid var(--rule);
  background: transparent;
}
.toggle[aria-pressed="true"] { color: var(--ink); }
.toggle[aria-pressed="true"]::before { background: var(--ink-muted); border-color: var(--ink-muted); }

.icon-btn { width: 28px; padding: 0; justify-content: center; color: var(--ink-2); }
.icon-btn:hover { color: var(--ink); }

.legend {
  display: flex;
  flex-wrap: wrap;
  gap: 4px 8px;
  align-items: center;
  padding: 6px 14px;
  border-bottom: 1px solid var(--hairline);
  background: var(--plane);
}
.chip {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  border: 1px solid transparent;
  border-radius: 999px;
  padding: 2px 9px 2px 6px;
  cursor: pointer;
  color: var(--ink-2);
  white-space: nowrap;
  height: 24px;
}
.chip:hover { border-color: var(--rule); }
.chip[aria-pressed="true"] { border-color: var(--ink-muted); color: var(--ink); background: var(--band); }
.chip .swatch { width: 10px; height: 10px; border-radius: 3px; flex: none; }
.chip .arrow { color: var(--edge-rev); font-weight: 700; }
.legend-sep { width: 1px; height: 15px; background: var(--rule); flex: none; margin: 0 3px; }
.chip .count { color: var(--ink-muted); font-variant-numeric: tabular-nums; }
.ramp { display: none; align-items: center; gap: 8px; color: var(--ink-2); }
.ramp .bar {
  width: 150px;
  height: 10px;
  border-radius: 3px;
  border: 1px solid var(--border);
}
body[data-color-mode="metric"] .ramp { display: flex; }
body[data-color-mode="metric"] .layer-chips { opacity: 0.45; }

main { flex: 1 1 auto; display: flex; min-height: 0; }
#canvas { flex: 1 1 auto; position: relative; overflow: hidden; background: var(--plane); min-width: 0; min-height: 220px; }
#graph { width: 100%; height: 100%; display: block; cursor: grab; touch-action: none; }
#graph.panning { cursor: grabbing; }

/* Camera controls live ON the canvas, where the camera is -- a "Fit" button
   parked in the header is the one control nobody finds when the graph is off
   the edge. */
.zoomer {
  position: absolute;
  right: 14px;
  bottom: 14px;
  z-index: 2;
  display: flex;
  align-items: center;
  gap: 1px;
  padding: 3px;
  border: 1px solid var(--rule);
  border-radius: 9px;
  background: var(--surface);
  box-shadow: 0 2px 10px var(--shadow);
}
.zoomer button {
  border: 0;
  background: transparent;
  border-radius: 6px;
  height: 26px;
  min-width: 26px;
  padding: 0 7px;
  color: var(--ink-2);
  line-height: 1;
}
.zoomer button:hover { background: var(--raised); color: var(--ink); }
.zoomer .level {
  font-size: 10.5px;
  color: var(--ink-muted);
  font-variant-numeric: tabular-nums;
  min-width: 40px;
  text-align: center;
}
.zoomer .sep { width: 1px; height: 16px; background: var(--hairline); margin: 0 3px; }
/* Says how to move the camera, then gets out of the way the first time you do. */
.pan-hint {
  position: absolute;
  left: 14px;
  bottom: 14px;
  z-index: 2;
  margin: 0;
  padding: 5px 10px;
  border: 1px solid var(--hairline);
  border-radius: 9px;
  background: var(--surface);
  box-shadow: 0 2px 10px var(--shadow);
  color: var(--ink-muted);
  font-size: 11px;
  pointer-events: none;
  transition: opacity 0.5s ease;
}
body.moved .pan-hint { opacity: 0; }

.band-rect { fill: var(--band); }
.band-label { fill: var(--ink-muted); font-size: 11px; font-weight: 600; letter-spacing: 0.04em; text-transform: uppercase; }

.edge { fill: none; stroke: var(--edge); stroke-width: 1.5; opacity: 0.75; }
.edge.optional { stroke-dasharray: 3 3; }
.edge.hidden { display: none; }
.edge.dim { opacity: 0.07; }
.edge.dep { stroke: var(--edge-dep); stroke-width: 2; opacity: 1; }
.edge.rev { stroke: var(--edge-rev); stroke-width: 2; stroke-dasharray: 5 3; opacity: 1; }
/* An anchor's own edge that the skeleton hides: direct and real, but implied by
   another path. Same hue, so direction still reads; thinner and softer, so it
   never outweighs the skeleton. */
.edge.implied.dep, .edge.implied.rev { stroke-width: 1.25; opacity: 0.55; }
/* A dependency that climbs the layer stack. This is what the tool is for, so it
   outranks the resting styling and stays visible with nothing selected. It is
   also thicker and dashed, because in a red/green-blind reading the hue alone
   would not separate it from an ordinary edge. */
.edge.inverted { stroke: var(--edge-rev); stroke-width: 2.25; stroke-dasharray: 6 3; opacity: 1; }
.edge.inverted.dim { opacity: 0.12; }

.node { cursor: pointer; }
/* Fill/ink come through custom properties, not presentation attributes: a
   stylesheet rule wins over an attribute, so a fill set by the metric painter
   with setAttribute would be ignored. */
.node .box {
  fill: var(--node-fill, var(--surface));
  stroke: var(--node-color);
  stroke-width: 1.5;
  rx: 7;
}
.node .stripe { fill: var(--node-color); }
.node.metric .stripe { display: none; }
.node .label { fill: var(--node-ink, var(--ink)); font-size: 11.5px; dominant-baseline: middle; }
.node.long .label { font-size: 10.5px; }
.node.dim { opacity: 0.22; }
.node.selected .box { stroke-width: 2.5; }
.node.selected .ring { stroke: var(--ink); stroke-width: 1; fill: none; opacity: 0.45; rx: 10; }
.node.match .box { stroke-dasharray: none; }
.node:focus { outline: none; }
.node:focus .ring, .node.hover .ring { stroke: var(--ink); stroke-width: 1; fill: none; opacity: 0.35; rx: 10; }

aside {
  width: 340px;
  flex: none;
  border-left: 1px solid var(--hairline);
  background: var(--surface);
  overflow-y: auto;
  padding: 14px 16px 28px;
}
aside h2 { font-size: 14px; margin: 0 0 2px; letter-spacing: -0.01em; }
aside h3 {
  font-size: 10.5px;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--ink-muted);
  margin: 18px 0 7px;
  font-weight: 600;
}
aside p { margin: 6px 0; color: var(--ink-2); }
.sub { color: var(--ink-2); font-size: 12px; margin-bottom: 8px; }
.tags { display: flex; flex-wrap: wrap; gap: 5px; margin: 8px 0 0; }
.tag {
  border: 1px solid var(--border);
  border-radius: 999px;
  padding: 1px 8px;
  font-size: 11.5px;
  color: var(--ink-2);
  display: inline-flex;
  align-items: center;
  gap: 5px;
}
.tag .swatch { width: 8px; height: 8px; border-radius: 2px; }
code, .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 11.5px; }
.metrics { display: grid; grid-template-columns: 1fr 1fr; gap: 1px; background: var(--hairline); border: 1px solid var(--hairline); border-radius: 7px; overflow: hidden; }
.metrics div { background: var(--surface); padding: 7px 9px; }
.metrics dt { font-size: 10.5px; color: var(--ink-muted); margin: 0; }
.metrics dd { margin: 1px 0 0; font-size: 16px; font-variant-numeric: tabular-nums; }
.pkglist { display: flex; flex-wrap: wrap; gap: 4px; }
.pkglist button {
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 2px 7px;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 11.5px;
  color: var(--ink-2);
  display: inline-flex;
  align-items: center;
  gap: 5px;
}
.pkglist button:hover { color: var(--ink); border-color: var(--ink-muted); }
.pkglist .swatch { width: 8px; height: 8px; border-radius: 2px; flex: none; }
.pkglist .qual { color: var(--ink-muted); font-family: system-ui, sans-serif; font-size: 10.5px; }
.pkglist button.up { border-color: var(--edge-rev); }
table.matrix { border-collapse: collapse; font-variant-numeric: tabular-nums; font-size: 11px; }
table.matrix th { color: var(--ink-muted); font-weight: 600; text-align: right; padding: 2px 5px; }
table.matrix th.row { text-align: left; white-space: nowrap; }
table.matrix td { text-align: right; padding: 2px 5px; color: var(--ink-2); }
table.matrix td.zero { color: var(--hairline); }
table.matrix td.self { color: var(--ink-muted); }
table.matrix td.up { color: var(--edge-rev); font-weight: 700; }
ol.rank { margin: 0; padding-left: 20px; color: var(--ink-2); }
ol.rank li { margin: 2px 0; }
ol.rank button { border: none; background: none; padding: 0; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 11.5px; color: var(--ink-2); }
ol.rank button:hover { color: var(--ink); text-decoration: underline; }
ol.rank .n { color: var(--ink-muted); font-variant-numeric: tabular-nums; }
ul.inversions { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 1px; background: var(--hairline); border: 1px solid var(--edge-rev); border-radius: 7px; overflow: hidden; }
ul.inversions li { background: var(--surface); padding: 6px 9px; }
ul.inversions .pair { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 11.5px; color: var(--ink); }
ul.inversions .where { font-size: 10.5px; color: var(--ink-muted); }
ul.inversions button { border: none; background: none; padding: 0; font: inherit; color: inherit; text-align: left; }
ul.inversions button:hover .pair { text-decoration: underline; }
.hint { color: var(--ink-muted); font-size: 11.5px; }

@media (max-width: 1180px) { aside { width: 300px; } }
@media (max-width: 900px) {
  /* Stacked, the panel must not eat the canvas: the graph keeps the larger share
     and the camera controls stay reachable, because on a narrow screen panning
     is the only way to read it. */
  main { flex-direction: column; }
  aside { width: auto; border-left: none; border-top: 1px solid var(--hairline); max-height: 40vh; }
  header { padding: 8px 10px; gap: 6px; }
  .legend { padding: 6px 10px; }
  .search { flex: 1 1 140px; }
  /* Packed left instead of justified: the spacer's job is to hold the toolbar
     apart on a wide row, and on a wrapping one it only buys blank rows and
     strands the last control on its own. */
  .spacer { display: none; }
}
@media (max-width: 620px) {
  .totals { display: none; }
  .pan-hint { display: none; }
  header { gap: 5px; }
  .field { padding: 0 3px 0 7px; gap: 5px; }
  .tools { display: contents; }
}
`;

/**
 * Inline so the file stays self-contained over `file://` -- currentColor keeps
 * them in step with the theme without a second rule per mode.
 */
const SEARCH_ICON =
  '<svg width="13" height="13" viewBox="0 0 16 16" aria-hidden="true" focusable="false">' +
  '<circle cx="7" cy="7" r="4.6" fill="none" stroke="currentColor" stroke-width="1.5"/>' +
  '<path d="M10.5 10.5 14 14" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>';

const THEME_ICON =
  '<svg width="14" height="14" viewBox="0 0 16 16" aria-hidden="true" focusable="false">' +
  '<circle cx="8" cy="8" r="6.1" fill="none" stroke="currentColor" stroke-width="1.5"/>' +
  '<path d="M8 1.9a6.1 6.1 0 0 1 0 12.2z" fill="currentColor"/></svg>';

const SCRIPT = String.raw`
(function () {
  var MODEL = JSON.parse(document.getElementById("workspace-model").textContent);
  var LAYER_COLORS = JSON.parse(document.getElementById("layer-colors").textContent);
  var SEQUENTIAL = JSON.parse(document.getElementById("sequential-ramp").textContent);
  var METRIC_LABELS = JSON.parse(document.getElementById("metric-labels").textContent);
  var SVGNS = "http://www.w3.org/2000/svg";

  var NODE_W = 168, NODE_H = 32, GAP_X = 16, ROW_GAP = 14;
  var BAND_LABEL = 24, BAND_PAD = 12, BAND_GAP = 44, MARGIN = 28;
  // A band wider than this wraps onto further rows. Without it the widest layer
  // (every app, tool and harness -- nothing imports them, so they all land in
  // one band) sets a canvas four times wider than it is tall, and Fit shrinks
  // the labels past reading.
  var MAX_COLS = 9;

  var pkgByName = {};
  MODEL.packages.forEach(function (p) { pkgByName[p.name] = p; });
  var layerIndexOf = {};
  var layerMeta = {};
  MODEL.layers.forEach(function (layer, i) {
    layerMeta[layer.id] = { index: i, label: layer.label, count: layer.packages.length };
    layer.packages.forEach(function (n) { layerIndexOf[n] = i; });
  });

  // An edge whose dependency sits in a HIGHER band: the architecture inverted.
  function isInverted(edge) {
    return layerIndexOf[edge.from] < layerIndexOf[edge.to];
  }
  var invertedEdges = MODEL.edges.filter(isInverted);

  // ---- reachability ------------------------------------------------------
  var down = {}, up = {};
  MODEL.packages.forEach(function (p) { down[p.name] = new Set(); up[p.name] = new Set(); });
  function walk(seedMap, key, edgeKey) {
    var seen = new Set();
    var stack = pkgByName[key][edgeKey].slice();
    while (stack.length) {
      var cur = stack.pop();
      if (seen.has(cur)) continue;
      seen.add(cur);
      stack = stack.concat(pkgByName[cur][edgeKey]);
    }
    seedMap[key] = seen;
  }
  MODEL.packages.forEach(function (p) {
    walk(down, p.name, "dependsOn");
    walk(up, p.name, "dependents");
  });

  // ---- layout ------------------------------------------------------------
  // Bands are the curated layers, foundations at the bottom, so an edge that
  // runs downward always means "depends on". Order within a band is
  // barycentre-sorted against every neighbour in the graph (deterministic: fixed
  // sweep count, alphabetical seed, index tie-break), then chunked into rows.
  function computeLayout() {
    var order = MODEL.layers.map(function (l) { return l.packages.slice(); });
    var neighbours = {};
    MODEL.packages.forEach(function (p) { neighbours[p.name] = []; });
    MODEL.edges.forEach(function (e) {
      neighbours[e.from].push(e.to);
      neighbours[e.to].push(e.from);
    });

    var pos = {};
    function reindex(li) {
      var row = order[li], d = Math.max(1, row.length - 1);
      row.forEach(function (n, i) { pos[n] = i / d; });
    }
    order.forEach(function (_, li) { reindex(li); });

    for (var sweep = 0; sweep < 24; sweep++) {
      for (var step = 0; step < order.length; step++) {
        var li = sweep % 2 === 0 ? step : order.length - 1 - step;
        var row = order[li], d = Math.max(1, row.length - 1);
        var keyed = row.map(function (n, i) {
          var ns = neighbours[n].filter(function (m) { return layerIndexOf[m] !== li; });
          var key = i / d;
          if (ns.length) {
            var sum = 0;
            ns.forEach(function (m) { sum += pos[m]; });
            key = sum / ns.length;
          }
          return { n: n, i: i, key: key };
        });
        keyed.sort(function (a, b) { return a.key - b.key || a.i - b.i; });
        order[li] = keyed.map(function (k) { return k.n; });
        reindex(li);
      }
    }

    // Balanced chunks rather than "fill to MAX_COLS then remainder": 17 members
    // become 9 + 8, not 9 + 8 vs a ragged 9 + 9 + ... on the next size up.
    var rowsPerBand = order.map(function (row) {
      var count = Math.max(1, Math.ceil(row.length / MAX_COLS));
      var perRow = Math.ceil(row.length / count);
      var rows = [];
      for (var i = 0; i < row.length; i += perRow) rows.push(row.slice(i, i + perRow));
      return rows;
    });

    var widest = 0;
    rowsPerBand.forEach(function (rows) {
      rows.forEach(function (r) { widest = Math.max(widest, r.length); });
    });
    var contentW = widest * NODE_W + (widest - 1) * GAP_X;

    var heights = rowsPerBand.map(function (rows) {
      return BAND_LABEL + rows.length * NODE_H + (rows.length - 1) * ROW_GAP + BAND_PAD;
    });

    var nodes = {}, bands = [];
    // Foundations at the bottom, so bands are placed from the last layer up.
    var y = MARGIN;
    for (var li = MODEL.layers.length - 1; li >= 0; li--) {
      var layer = MODEL.layers[li];
      var rows = rowsPerBand[li];
      bands.push({
        id: layer.id, label: layer.label, y: y, h: heights[li],
        w: contentW, count: order[li].length,
      });
      rows.forEach(function (row, ri) {
        var rowW = row.length * NODE_W + (row.length - 1) * GAP_X;
        var startX = MARGIN + (contentW - rowW) / 2;
        var rowY = y + BAND_LABEL + ri * (NODE_H + ROW_GAP);
        row.forEach(function (n, i) {
          nodes[n] = {
            x: startX + i * (NODE_W + GAP_X), y: rowY,
            w: NODE_W, h: NODE_H, layerIndex: li,
          };
        });
      });
      y += heights[li] + BAND_GAP;
    }

    return {
      nodes: nodes,
      bands: bands,
      width: contentW + MARGIN * 2,
      height: y - BAND_GAP + MARGIN,
    };
  }

  var L = computeLayout();

  function edgePath(a, b) {
    if (a.y === b.y) {
      var y = a.y + a.h, dip = 30;
      return "M" + (a.x + a.w / 2) + "," + y +
        " C" + (a.x + a.w / 2) + "," + (y + dip) +
        " " + (b.x + b.w / 2) + "," + (y + dip) +
        " " + (b.x + b.w / 2) + "," + y;
    }
    var downward = b.y > a.y;
    var x1 = a.x + a.w / 2, y1 = downward ? a.y + a.h : a.y;
    var x2 = b.x + b.w / 2, y2 = downward ? b.y : b.y + b.h;
    var d = Math.max(26, Math.abs(y2 - y1) * 0.36) * (downward ? 1 : -1);
    return "M" + x1 + "," + y1 + " C" + x1 + "," + (y1 + d) + " " + x2 + "," + (y2 - d) + " " + x2 + "," + y2;
  }

  // ---- colour ------------------------------------------------------------
  function isDark() {
    var stamped = document.documentElement.getAttribute("data-theme");
    if (stamped) return stamped === "dark";
    return window.matchMedia("(prefers-color-scheme: dark)").matches;
  }
  function layerColor(layerId) {
    var palette = LAYER_COLORS[isDark() ? "dark" : "light"];
    return palette[layerMeta[layerId].index % palette.length];
  }
  // These are ordinal marks, not a continuous field, so the step nearest the
  // surface still has to be visible: light runs step 250 -> 700, dark runs step
  // 600 -> 100. More always means further from the surface.
  var LIGHT_FLOOR = 3, DARK_FLOOR = 10;
  function sequentialColor(t) {
    var clamped = Math.max(0, Math.min(1, t));
    if (!isDark()) {
      return SEQUENTIAL[LIGHT_FLOOR + Math.round(clamped * (SEQUENTIAL.length - 1 - LIGHT_FLOOR))];
    }
    return SEQUENTIAL[DARK_FLOOR - Math.round(clamped * DARK_FLOOR)];
  }
  // Light mode darkens as t rises, dark mode brightens -- so the label flips the
  // opposite way.
  function inkOn(t) {
    var clamped = Math.max(0, Math.min(1, t));
    if (isDark()) return clamped > 0.7 ? "#0b0b0b" : "#ffffff";
    return clamped > 0.44 ? "#ffffff" : "#0b0b0b";
  }

  var state = {
    selected: null,
    hovered: null,
    query: "",
    edgeSet: "structural",
    showDev: true,
    colorBy: "layer",
    focus: "both",
    layer: null,
    inversionsOnly: false,
  };

  function metricValue(pkg) { return pkg.metrics[state.colorBy]; }
  function isMeasured(pkg, metric) {
    var v = pkg.metrics[metric];
    return v !== null && v !== undefined && isFinite(v);
  }
  function hasMetric(pkg) { return isMeasured(pkg, state.colorBy); }
  function metricRange() {
    var lo = Infinity, hi = -Infinity;
    MODEL.packages.forEach(function (p) {
      if (!hasMetric(p)) return;
      var v = metricValue(p);
      if (v < lo) lo = v;
      if (v > hi) hi = v;
    });
    return { lo: lo === Infinity ? 0 : lo, hi: hi === -Infinity ? 0 : hi };
  }
  // The count metrics are long-tailed -- one package has 30 direct dependents
  // and most have none -- so a linear ramp collapses everything but the outliers
  // into one step. Square-root easing keeps the order and spreads the low end;
  // the legend says so. Instability is already a bounded ratio and stays linear.
  function metricT(pkg, range) {
    var v = metricValue(pkg);
    if (state.colorBy === "instability") {
      return range.hi === range.lo ? 0 : (v - range.lo) / (range.hi - range.lo);
    }
    return range.hi <= 0 ? 0 : Math.sqrt(Math.max(0, v) / range.hi);
  }

  // ---- svg ---------------------------------------------------------------
  var svg = document.getElementById("graph");
  var viewport = document.createElementNS(SVGNS, "g");
  var bandLayer = document.createElementNS(SVGNS, "g");
  var edgeLayer = document.createElementNS(SVGNS, "g");
  var nodeLayer = document.createElementNS(SVGNS, "g");
  viewport.appendChild(bandLayer);
  viewport.appendChild(edgeLayer);
  viewport.appendChild(nodeLayer);
  svg.appendChild(viewport);

  L.bands.forEach(function (band) {
    var rect = document.createElementNS(SVGNS, "rect");
    rect.setAttribute("class", "band-rect");
    rect.setAttribute("x", MARGIN - 12);
    rect.setAttribute("y", band.y);
    rect.setAttribute("width", band.w + 24);
    rect.setAttribute("height", band.h);
    rect.setAttribute("rx", 10);
    bandLayer.appendChild(rect);

    var text = document.createElementNS(SVGNS, "text");
    text.setAttribute("class", "band-label");
    text.setAttribute("x", MARGIN - 2);
    text.setAttribute("y", band.y + 15);
    text.textContent = band.label + "  ·  " + band.count;
    bandLayer.appendChild(text);
  });

  var edgeEls = MODEL.edges.map(function (edge) {
    var path = document.createElementNS(SVGNS, "path");
    path.setAttribute("d", edgePath(L.nodes[edge.from], L.nodes[edge.to]));
    path.setAttribute("class", "edge" + (edge.optional ? " optional" : ""));
    var title = document.createElementNS(SVGNS, "title");
    title.textContent = edge.from + " → " + edge.to + " (" + edge.kind + ")" +
      (isInverted(edge) ? " — layer inversion" : "");
    path.appendChild(title);
    edgeLayer.appendChild(path);
    return { edge: edge, el: path, inverted: isInverted(edge) };
  });

  var nodeEls = {};
  MODEL.packages.forEach(function (pkg) {
    var box = L.nodes[pkg.name];
    var g = document.createElementNS(SVGNS, "g");
    g.setAttribute("class", "node" + (pkg.name.length > 21 ? " long" : ""));
    g.setAttribute("transform", "translate(" + box.x + "," + box.y + ")");
    g.setAttribute("tabindex", "0");
    g.setAttribute("role", "button");
    g.setAttribute("data-pkg", pkg.name);
    g.setAttribute("aria-label", pkg.name + ", " + layerMeta[pkg.layer].label);

    var ring = document.createElementNS(SVGNS, "rect");
    ring.setAttribute("class", "ring");
    ring.setAttribute("x", -3);
    ring.setAttribute("y", -3);
    ring.setAttribute("width", box.w + 6);
    ring.setAttribute("height", box.h + 6);
    ring.setAttribute("fill", "none");
    ring.setAttribute("stroke", "none");
    g.appendChild(ring);

    var rect = document.createElementNS(SVGNS, "rect");
    rect.setAttribute("class", "box");
    rect.setAttribute("width", box.w);
    rect.setAttribute("height", box.h);
    g.appendChild(rect);

    var stripe = document.createElementNS(SVGNS, "rect");
    stripe.setAttribute("class", "stripe");
    stripe.setAttribute("x", 1.5);
    stripe.setAttribute("y", 6);
    stripe.setAttribute("width", 3.5);
    stripe.setAttribute("height", box.h - 12);
    stripe.setAttribute("rx", 2);
    g.appendChild(stripe);

    var label = document.createElementNS(SVGNS, "text");
    label.setAttribute("class", "label");
    label.setAttribute("x", 14);
    label.setAttribute("y", box.h / 2);
    label.textContent = pkg.name;
    g.appendChild(label);

    var title = document.createElementNS(SVGNS, "title");
    title.textContent = pkg.name + " — " + layerMeta[pkg.layer].label;
    g.appendChild(title);

    // No click listener: while the <svg> holds the pointer capture it opened on
    // pointerdown, the click event is dispatched to the <svg>, not here.
    // endDrag does the selection instead.
    g.addEventListener("keydown", function (event) {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        select(state.selected === pkg.name ? null : pkg.name);
      }
    });
    g.addEventListener("mouseenter", function () { state.hovered = pkg.name; paint(); });
    g.addEventListener("mouseleave", function () { state.hovered = null; paint(); });

    nodeLayer.appendChild(g);
    nodeEls[pkg.name] = { g: g, rect: rect, stripe: stripe, label: label, ring: ring };
  });

  // ---- painting ----------------------------------------------------------
  function focusSets(name) {
    var pkg = pkgByName[name];
    if (state.focus === "neighbours") {
      return { down: new Set(pkg.dependsOn), up: new Set(pkg.dependents) };
    }
    if (state.focus === "dependencies") return { down: down[name], up: new Set() };
    if (state.focus === "dependents") return { down: new Set(), up: up[name] };
    return { down: down[name], up: up[name] };
  }

  var invertedEnds = new Set();
  invertedEdges.forEach(function (e) { invertedEnds.add(e.from); invertedEnds.add(e.to); });

  function paint() {
    var range = metricRange();
    var byMetric = state.colorBy !== "layer";
    document.body.setAttribute("data-color-mode", byMetric ? "metric" : "layer");

    MODEL.packages.forEach(function (pkg) {
      var el = nodeEls[pkg.name];
      el.g.classList.toggle("metric", byMetric);
      if (byMetric) {
        var t = metricT(pkg, range);
        el.g.style.setProperty("--node-color", sequentialColor(t));
        el.g.style.setProperty("--node-fill", sequentialColor(t));
        el.g.style.setProperty("--node-ink", inkOn(t));
      } else {
        el.g.style.setProperty("--node-color", layerColor(pkg.layer));
        el.g.style.removeProperty("--node-fill");
        el.g.style.removeProperty("--node-ink");
      }
    });

    var query = state.query.trim().toLowerCase();
    var anchor = state.selected || state.hovered;
    var sets = anchor ? focusSets(anchor) : null;

    function nodeState(name) {
      if (query) return name.toLowerCase().indexOf(query) !== -1 ? "match" : "dim";
      if (state.layer && pkgByName[name].layer !== state.layer) return "dim";
      if (state.inversionsOnly && !invertedEnds.has(name)) return "dim";
      if (!anchor) return "plain";
      if (name === anchor) return "anchor";
      if (sets.down.has(name) || sets.up.has(name)) return "near";
      return "dim";
    }

    MODEL.packages.forEach(function (pkg) {
      var el = nodeEls[pkg.name];
      var s = nodeState(pkg.name);
      el.g.classList.toggle("dim", s === "dim");
      el.g.classList.toggle("match", s === "match");
      el.g.classList.toggle("selected", pkg.name === state.selected);
      el.g.classList.toggle("hover", pkg.name === state.hovered && pkg.name !== state.selected);
    });

    edgeEls.forEach(function (entry) {
      var edge = entry.edge, el = entry.el;
      // The skeleton drops an edge another path already implies. That is right
      // for the resting graph and wrong for the package the reader just
      // anchored: it would draw one of that package's OWN direct dependencies as
      // a detour through the intermediary, which reads as "not a direct
      // dependency" -- the opposite of the truth. An anchor always shows its
      // real neighbours; "implied" then marks the ones the skeleton would hide.
      var ownEdge = !!anchor && (edge.from === anchor || edge.to === anchor);
      var implied = ownEdge && !edge.structural && state.edgeSet !== "all";
      // An inversion is never hidden by the skeleton: it is the finding.
      var visible =
        (state.edgeSet === "all" || edge.structural || ownEdge || entry.inverted) &&
        (state.showDev || edge.kind !== "dev") &&
        (!state.inversionsOnly || entry.inverted);
      el.classList.toggle("hidden", !visible);
      el.classList.remove("dep", "rev", "dim", "implied", "inverted");
      if (!visible) return;
      if (implied) el.classList.add("implied");
      if (entry.inverted) el.classList.add("inverted");

      if (query) {
        var both = nodeState(edge.from) === "match" && nodeState(edge.to) === "match";
        if (!both) el.classList.add("dim");
        return;
      }
      if (!anchor) {
        // An edge is only as bright as its dimmer end, which covers every
        // standing filter at once.
        if (state.layer &&
            (nodeState(edge.from) === "dim" || nodeState(edge.to) === "dim")) {
          el.classList.add("dim");
        }
        return;
      }
      var downSide = (edge.from === anchor || sets.down.has(edge.from)) && sets.down.has(edge.to);
      var upSide = (edge.to === anchor || sets.up.has(edge.to)) && sets.up.has(edge.from);
      if (entry.inverted) return; // keeps its own paint
      if (downSide) el.classList.add("dep");
      else if (upSide) el.classList.add("rev");
      else el.classList.add("dim");
    });

    renderPanel();
    renderRamp(range);
  }

  // ---- panel -------------------------------------------------------------
  var panel = document.getElementById("panel");

  function el(tag, attrs, kids) {
    var node = document.createElement(tag);
    Object.keys(attrs || {}).forEach(function (k) {
      if (k === "class") node.className = attrs[k];
      else if (k === "text") node.textContent = attrs[k];
      else node.setAttribute(k, attrs[k]);
    });
    (kids || []).forEach(function (kid) { if (kid) node.appendChild(kid); });
    return node;
  }
  function swatch(layerId) {
    var s = el("span", { class: "swatch" });
    s.style.background = layerColor(layerId);
    return s;
  }
  function pkgButton(name, qualifier, climbs) {
    var button = el("button", { type: "button", class: climbs ? "up" : "" },
      [swatch(pkgByName[name].layer), el("span", { text: name })]);
    if (qualifier) button.appendChild(el("span", { class: "qual", text: qualifier }));
    button.addEventListener("click", function () { select(name); });
    return button;
  }
  function rank(metric, limit, ascending, format) {
    var ranked = MODEL.packages.filter(function (p) { return isMeasured(p, metric); }).sort(function (a, b) {
      var delta = ascending ? a.metrics[metric] - b.metrics[metric] : b.metrics[metric] - a.metrics[metric];
      return delta || (a.name < b.name ? -1 : 1);
    }).slice(0, limit);
    var list = el("ol", { class: "rank" });
    ranked.forEach(function (p) {
      var button = el("button", { type: "button", text: p.name });
      button.addEventListener("click", function () { select(p.name); });
      var value = format ? format(p) : String(p.metrics[metric]);
      list.appendChild(el("li", {}, [button, el("span", { class: "n", text: "  " + value })]));
    });
    return list;
  }

  function renderOverview() {
    panel.replaceChildren();
    panel.appendChild(el("h2", { text: "Workspace overview" }));
    panel.appendChild(el("p", { class: "sub", text:
      MODEL.totals.packages + " packages · " + MODEL.totals.edges + " direct internal dependencies · " +
      MODEL.totals.structuralEdges + " structural" }));
    panel.appendChild(el("p", { class: "hint", text:
      "Click a package to trace its cones. Edges run downward: consumer → dependency. " +
      "An anchored package also shows its own direct edges that the skeleton hides, drawn thinner." }));
    panel.appendChild(el("p", { class: "hint", text:
      MODEL.totals.edges - MODEL.totals.runtimeEdges + " of these edges are declared as " +
      "devDependencies, which is how a package here imports another package's TypeScript source. " +
      "Turn “Dev deps” off to see only what the published packages depend on at runtime." }));

    panel.appendChild(el("h3", { text: "Layer inversions · " + invertedEdges.length }));
    if (invertedEdges.length) {
      panel.appendChild(el("p", { class: "hint", text:
        "A dependency pointing UP the stack — a package reaching into something meant to sit above " +
        "it. Drawn in red on the graph, whatever else is selected." }));
      var list = el("ul", { class: "inversions" });
      invertedEdges.forEach(function (e) {
        var button = el("button", { type: "button" }, [
          el("div", { class: "pair", text: e.from + "  ↑  " + e.to }),
          el("div", { class: "where", text:
            layerMeta[pkgByName[e.from].layer].label + " → " +
            layerMeta[pkgByName[e.to].layer].label + " · " + e.kind }),
        ]);
        button.addEventListener("click", function () { select(e.from); });
        list.appendChild(el("li", {}, [button]));
      });
      panel.appendChild(list);
    } else {
      panel.appendChild(el("p", { class: "hint", text:
        "None. Every internal dependency points down the layer stack." }));
    }

    panel.appendChild(el("h3", { text: "Cross-layer dependencies" }));
    var internal = {};
    MODEL.edges.forEach(function (e) {
      var a = pkgByName[e.from].layer, b = pkgByName[e.to].layer;
      if (a === b) internal[a] = (internal[a] || 0) + 1;
    });
    var counts = {};
    MODEL.layerEdges.forEach(function (e) { counts[e.from + "\0" + e.to] = e.count; });

    var table = el("table", { class: "matrix" });
    var head = el("tr", {}, [el("th", { class: "row", text: "consumer ↓ / dep →" })]);
    MODEL.layers.forEach(function (layer, i) { head.appendChild(el("th", { text: String(i + 1) })); });
    table.appendChild(head);
    MODEL.layers.forEach(function (row, ri) {
      var tr = el("tr", {}, [el("th", { class: "row", text: (ri + 1) + ". " + row.label })]);
      MODEL.layers.forEach(function (col, ci) {
        if (ri === ci) {
          var self = internal[row.id] || 0;
          tr.appendChild(el("td", { class: self ? "self" : "zero", text: self ? String(self) : "·" }));
          return;
        }
        var n = counts[row.id + "\0" + col.id] || 0;
        // Above the diagonal is an inversion: a lower layer depending on a
        // higher one. Marked, so the shape of the workspace reads off the
        // triangle rather than off a legend.
        var cls = !n ? "zero" : ci > ri ? "up" : "";
        tr.appendChild(el("td", { class: cls, text: n ? String(n) : "·" }));
      });
      table.appendChild(tr);
    });
    panel.appendChild(table);

    panel.appendChild(el("h3", { text: "Most depended upon (fan-in)" }));
    panel.appendChild(rank("fanIn", 6));
    panel.appendChild(el("h3", { text: "Widest blast radius" }));
    panel.appendChild(rank("blastRadius", 6));
    panel.appendChild(el("h3", { text: "Largest dependency cone" }));
    panel.appendChild(rank("cone", 6));
  }

  function renderDetail(name) {
    var pkg = pkgByName[name];
    panel.replaceChildren();
    panel.appendChild(el("h2", { class: "mono", text: pkg.name }));
    panel.appendChild(el("p", { class: "sub", text: "v" + pkg.version + " · " + pkg.targetKind }));
    panel.appendChild(el("div", { class: "tags" }, [
      el("span", { class: "tag" }, [swatch(pkg.layer), el("span", { text: layerMeta[pkg.layer].label })]),
      el("span", { class: "tag mono", text: pkg.dir }),
    ]));
    if (pkg.description) panel.appendChild(el("p", { text: pkg.description }));

    panel.appendChild(el("h3", { text: "Metrics" }));
    var grid = el("dl", { class: "metrics" });
    [
      ["Fan-in", pkg.metrics.fanIn, "direct dependents"],
      ["Fan-out", pkg.metrics.fanOut, "direct dependencies"],
      ["Cone", pkg.metrics.cone, "transitive dependencies"],
      ["Blast radius", pkg.metrics.blastRadius, "transitive dependents"],
      ["Depth", pkg.metrics.depth, "longest path to a leaf"],
      ["Instability", pkg.metrics.instability, "0 = depended upon, 1 = depends"],
    ].forEach(function (row) {
      grid.appendChild(el("div", {}, [
        el("dt", { text: row[0], title: row[2] }),
        el("dd", { text: String(row[1]) }),
      ]));
    });
    panel.appendChild(grid);

    var qualifiers = {}, climbs = {};
    MODEL.edges.forEach(function (e) {
      if (e.from !== name) return;
      var parts = [];
      if (e.optional) parts.push("optional");
      if (e.kind !== "runtime") parts.push(e.kind);
      if (parts.length) qualifiers[e.to] = parts.join(", ");
      if (isInverted(e)) climbs[e.to] = true;
    });

    panel.appendChild(el("h3", { text: "Depends on · " + pkg.dependsOn.length +
      (pkg.metrics.cone !== pkg.dependsOn.length ? " direct, " + pkg.metrics.cone + " transitive" : "") }));
    panel.appendChild(pkg.dependsOn.length
      ? el("div", { class: "pkglist" }, pkg.dependsOn.map(function (n) {
          return pkgButton(n, qualifiers[n], climbs[n]);
        }))
      : el("p", { class: "hint", text: "No internal dependencies — a leaf of the workspace." }));

    panel.appendChild(el("h3", { text: "Depended on by · " + pkg.dependents.length +
      (pkg.metrics.blastRadius !== pkg.dependents.length ? " direct, " + pkg.metrics.blastRadius + " transitive" : "") }));
    panel.appendChild(pkg.dependents.length
      ? el("div", { class: "pkglist" }, pkg.dependents.map(function (n) {
          return pkgButton(n, null, layerIndexOf[n] < layerIndexOf[name]);
        }))
      : el("p", { class: "hint", text: "Nothing in the workspace depends on this — a root." }));

    var back = el("button", { type: "button", text: "Clear selection" });
    back.style.marginTop = "18px";
    back.addEventListener("click", function () { select(null); });
    panel.appendChild(back);
  }

  // Hovering repaints the graph constantly; rebuilding the panel with it would
  // reset its scroll position under the cursor. Only selection (and theme, which
  // restyles the swatches) changes it.
  var panelKey = null;
  function renderPanel() {
    var key = (state.selected || "") + "|" + isDark();
    if (key === panelKey) return;
    panelKey = key;
    if (state.selected) renderDetail(state.selected);
    else renderOverview();
  }

  function renderRamp(range) {
    var bar = document.getElementById("ramp-bar");
    var lo = document.getElementById("ramp-lo");
    var hi = document.getElementById("ramp-hi");
    var name = document.getElementById("ramp-name");
    if (state.colorBy === "layer") return;
    var stops = [];
    for (var i = 0; i <= 10; i++) stops.push(sequentialColor(i / 10) + " " + (i * 10) + "%");
    bar.style.background = "linear-gradient(90deg, " + stops.join(", ") + ")";
    lo.textContent = String(range.lo);
    hi.textContent = String(range.hi);
    var chosen = null;
    METRIC_LABELS.forEach(function (m) { if (m.id === state.colorBy) chosen = m.label; });
    name.textContent = (chosen || state.colorBy) + (state.colorBy === "instability" ? "" : "  · √ scale");
  }

  function select(name) {
    state.selected = name;
    if (name) {
      state.query = "";
      document.getElementById("search").value = "";
    }
    paint();
    if (name) nodeEls[name].g.focus({ preventScroll: true });
  }

  // ---- pan & zoom --------------------------------------------------------
  // No viewBox, so SVG user units are CSS pixels and the camera is a plain
  // screen-space transform.
  var MIN_K = 0.15, MAX_K = 3, KEEP = 90;
  var view = { x: 0, y: 0, k: 1 };
  // Set once the user moves the camera themselves, so a window resize re-clamps
  // instead of yanking the graph back to Fit under them.
  var viewTouched = false;

  function applyView() {
    viewport.setAttribute(
      "transform",
      "translate(" + view.x.toFixed(2) + "," + view.y.toFixed(2) + ") scale(" + view.k.toFixed(4) + ")",
    );
    zoomLevel.textContent = Math.round(view.k * 100) + "%";
  }

  // Always leave KEEP pixels of the graph inside the canvas. Unbounded panning
  // on a plane this wide loses it off an edge with no cue for which way it went.
  function clampView() {
    var rect = svg.getBoundingClientRect();
    var w = L.width * view.k, h = L.height * view.k;
    var loX = KEEP - w, hiX = Math.max(loX, rect.width - KEEP);
    var loY = KEEP - h, hiY = Math.max(loY, rect.height - KEEP);
    view.x = Math.min(Math.max(view.x, loX), hiX);
    view.y = Math.min(Math.max(view.y, loY), hiY);
  }

  function commitView(touched) {
    if (touched && !viewTouched) {
      viewTouched = true;
      document.body.classList.add("moved");
    }
    clampView();
    applyView();
  }

  function fit() {
    var rect = svg.getBoundingClientRect();
    if (!rect.width || !rect.height) return;
    var pad = 20;
    var k = Math.min((rect.width - pad * 2) / L.width, (rect.height - pad * 2) / L.height, 1.6);
    view.k = Math.max(MIN_K, k);
    view.x = (rect.width - L.width * view.k) / 2;
    view.y = (rect.height - L.height * view.k) / 2;
    viewTouched = false;
    applyView();
  }

  function zoomAbout(px, py, next) {
    next = Math.max(MIN_K, Math.min(MAX_K, next));
    if (next === view.k) return;
    view.x = px - (px - view.x) * (next / view.k);
    view.y = py - (py - view.y) * (next / view.k);
    view.k = next;
    commitView(true);
  }

  function zoomCentre(factor) {
    var rect = svg.getBoundingClientRect();
    zoomAbout(rect.width / 2, rect.height / 2, view.k * factor);
  }

  var dragging = null;
  svg.addEventListener("pointerdown", function (event) {
    if (event.button !== 0) return;
    // Capture the real target now: setPointerCapture retargets every later event
    // to the <svg> -- the click included -- so neither pointerup nor a listener
    // on the node itself can tell a node hit from a background hit. Selection is
    // resolved in endDrag from this remembered target.
    dragging = {
      x: event.clientX, y: event.clientY,
      vx: view.x, vy: view.y,
      moved: false, target: event.target,
    };
    svg.setPointerCapture(event.pointerId);
    svg.classList.add("panning");
  });
  svg.addEventListener("pointermove", function (event) {
    if (!dragging) return;
    var dx = event.clientX - dragging.x, dy = event.clientY - dragging.y;
    if (!dragging.moved && Math.abs(dx) + Math.abs(dy) <= 3) return; // still a click
    dragging.moved = true;
    view.x = dragging.vx + dx;
    view.y = dragging.vy + dy;
    commitView(true);
  });
  function nodeUnder(target) {
    var hit = target && target.closest ? target.closest(".node") : null;
    return hit ? hit.getAttribute("data-pkg") : null;
  }
  function endDrag() {
    if (!dragging) return;
    if (!dragging.moved) {
      var hit = nodeUnder(dragging.target);
      select(hit && hit === state.selected ? null : hit);
    }
    dragging = null;
    svg.classList.remove("panning");
  }
  svg.addEventListener("pointerup", endDrag);
  svg.addEventListener("pointercancel", endDrag);
  svg.addEventListener("wheel", function (event) {
    event.preventDefault();
    var rect = svg.getBoundingClientRect();
    // A trackpad pinch arrives as a ctrl-wheel and ⌘/ctrl + wheel is its mouse
    // equivalent; plain wheel scrolls the camera, because on a plane wider than
    // the canvas a two-finger swipe means "move", not "zoom".
    if (event.ctrlKey || event.metaKey) {
      zoomAbout(
        event.clientX - rect.left,
        event.clientY - rect.top,
        view.k * Math.exp(-event.deltaY * 0.0015),
      );
      return;
    }
    var unit = event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? rect.height : 1;
    view.x -= event.deltaX * unit;
    view.y -= event.deltaY * unit;
    commitView(true);
  }, { passive: false });

  var zoomLevel = document.getElementById("zoom-level");
  document.getElementById("zoom-in").addEventListener("click", function () { zoomCentre(1.25); });
  document.getElementById("zoom-out").addEventListener("click", function () { zoomCentre(1 / 1.25); });

  // ---- controls ----------------------------------------------------------
  document.getElementById("search").addEventListener("input", function (event) {
    state.query = event.target.value;
    if (state.query) state.selected = null;
    paint();
  });
  document.getElementById("edge-set").addEventListener("change", function (event) {
    state.edgeSet = event.target.value;
    paint();
  });
  var devToggle = document.getElementById("dev");
  devToggle.addEventListener("click", function () {
    state.showDev = !state.showDev;
    devToggle.setAttribute("aria-pressed", state.showDev ? "true" : "false");
    paint();
  });
  document.getElementById("color-by").addEventListener("change", function (event) {
    state.colorBy = event.target.value;
    paint();
  });
  document.getElementById("focus").addEventListener("change", function (event) {
    state.focus = event.target.value;
    paint();
  });
  document.getElementById("fit").addEventListener("click", fit);
  document.getElementById("theme").addEventListener("click", function () {
    document.documentElement.setAttribute("data-theme", isDark() ? "light" : "dark");
    paint();
  });
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", function () { paint(); });

  MODEL.layers.forEach(function (layer) {
    var chip = el("button", { class: "chip", type: "button", "aria-pressed": "false" }, [
      swatch(layer.id),
      el("span", { text: layer.label }),
      el("span", { class: "count", text: String(layer.packages.length) }),
    ]);
    chip.addEventListener("click", function () {
      state.layer = state.layer === layer.id ? null : layer.id;
      Array.prototype.forEach.call(document.querySelectorAll(".layer-chips .chip"), function (other) {
        other.setAttribute("aria-pressed", "false");
      });
      chip.setAttribute("aria-pressed", state.layer === layer.id ? "true" : "false");
      paint();
    });
    document.querySelector(".layer-chips").appendChild(chip);
  });

  // Orthogonal to the layer chips, not one more of them.
  var inversions = document.getElementById("inversions");
  if (inversions) {
    inversions.addEventListener("click", function () {
      state.inversionsOnly = !state.inversionsOnly;
      inversions.setAttribute("aria-pressed", state.inversionsOnly ? "true" : "false");
      paint();
    });
  }

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") {
      state.query = "";
      state.layer = null;
      state.inversionsOnly = false;
      document.getElementById("search").value = "";
      Array.prototype.forEach.call(document.querySelectorAll(".legend .chip"), function (chip) {
        chip.setAttribute("aria-pressed", "false");
      });
      select(null);
    }
    var typing = document.activeElement === document.getElementById("search");
    if (event.key === "/" && !typing) {
      event.preventDefault();
      document.getElementById("search").focus();
    }
    if (typing) return;
    if (event.key === "0") { event.preventDefault(); fit(); }
    else if (event.key === "+" || event.key === "=") { event.preventDefault(); zoomCentre(1.25); }
    else if (event.key === "-" || event.key === "_") { event.preventDefault(); zoomCentre(1 / 1.25); }
  });
  // Re-fit only while the camera is still the one we chose; once the user has
  // moved it, a resize just re-clamps so their framing survives the window
  // change. This watches the CANVAS, not the window: the panel filling in, the
  // toolbar wrapping to a second row and a webfont landing each resize the
  // canvas without a window resize.
  function reframe() {
    if (viewTouched) commitView(false);
    else fit();
  }
  if (window.ResizeObserver) new ResizeObserver(reframe).observe(document.getElementById("canvas"));
  else window.addEventListener("resize", reframe);

  paint();
  fit();
  // The hint has said its piece by the time anyone has read it; the graph gets
  // the corner back.
  setTimeout(function () { document.body.classList.add("moved"); }, 9000);
})();
`;

export function renderHtml(model: Model): string {
  const metricOptions = METRICS.map(
    metric =>
      `<option value="${metric.id}"${metric.id === 'layer' ? ' selected' : ''}>${
        metric.short ?? metric.label
      }</option>`,
  ).join('\n            ');
  const inversions = model.layerViolations.length;

  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${model.meta.title} · ${model.totals.packages} packages</title>
<style>${STYLE}</style>
</head>
<body data-color-mode="layer">
  <header>
    <span class="brand">
      <h1>${model.meta.title}</h1>
      <span class="totals">${model.totals.packages} packages · ${model.totals.edges} deps · ${model.totals.structuralEdges} structural</span>
    </span>
    <span class="spacer"></span>
    <span class="search">
      ${SEARCH_ICON}
      <input id="search" type="search" placeholder="Filter packages  (/)" aria-label="Filter packages">
    </span>
    <span class="tools">
      <label class="field"><span class="k">Edges</span>
        <select id="edge-set" aria-label="Which dependency edges to draw">
          <option value="structural" selected>Structural</option>
          <option value="all">All direct</option>
        </select>
      </label>
      <button id="dev" type="button" class="toggle" aria-pressed="true"
              title="Include dependencies declared as devDependencies — in this repo that is how a package imports another package's source">Dev deps</button>
      <label class="field"><span class="k">Colour</span>
        <select id="color-by" aria-label="Colour packages by">
            ${metricOptions}
        </select>
      </label>
      <label class="field"><span class="k">Focus</span>
        <select id="focus" aria-label="What a selected package lights up">
          <option value="both" selected>Both cones</option>
          <option value="neighbours">Direct neighbours</option>
          <option value="dependencies">Dependencies</option>
          <option value="dependents">Dependents</option>
        </select>
      </label>
    </span>
    <button id="theme" type="button" class="icon-btn" title="Switch light / dark" aria-label="Switch light / dark">${THEME_ICON}</button>
  </header>

  <div class="legend">
    <span class="cap">Layers</span>
    <span class="layer-chips" style="display:contents"></span>${
      inversions
        ? `
    <span class="legend-sep"></span>
    <button id="inversions" type="button" class="chip" aria-pressed="false"
            title="Show only the dependencies that point up the layer stack">
      <span class="arrow" aria-hidden="true">↑</span>
      <span>Layer inversions</span>
      <span class="count">${inversions}</span>
    </button>`
        : ''
    }
    <span class="spacer"></span>
    <span class="ramp">
      <span id="ramp-name"></span>
      <span id="ramp-lo">0</span>
      <span class="bar" id="ramp-bar"></span>
      <span id="ramp-hi">0</span>
    </span>
  </div>

  <main>
    <div id="canvas">
      <svg id="graph" xmlns="http://www.w3.org/2000/svg"></svg>
      <p class="pan-hint">Drag to pan · scroll to move · pinch or ⌘-scroll to zoom</p>
      <div class="zoomer">
        <button id="zoom-out" type="button" title="Zoom out (−)" aria-label="Zoom out">−</button>
        <span class="level" id="zoom-level">100%</span>
        <button id="zoom-in" type="button" title="Zoom in (+)" aria-label="Zoom in">+</button>
        <span class="sep"></span>
        <button id="fit" type="button" title="Fit the whole graph (0)">Fit</button>
      </div>
    </div>
    <aside id="panel"></aside>
  </main>

<script type="application/json" id="workspace-model">${embedJson(model)}</script>
<script type="application/json" id="layer-colors">${embedJson(LAYER_COLORS)}</script>
<script type="application/json" id="sequential-ramp">${embedJson(SEQUENTIAL)}</script>
<script type="application/json" id="metric-labels">${embedJson(METRICS)}</script>
<script>${SCRIPT}</script>
</body>
</html>
`;
}
