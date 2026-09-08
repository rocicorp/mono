/**
 * The exported-metrics overlay for the workspace model.
 *
 * `zero-cache` serves an OpenTelemetry scrape, and what it exports was knowable
 * only by reading the ~30 files that declare instruments. This module extracts
 * the catalog straight from those declaration sites, which in this repo ARE the
 * single source of truth -- there is no separate renderer to read back, the way
 * rindle's Rust conformance tests read back a rendered `/metrics` document.
 *
 * Every instrument goes through the small helper layer in
 * `zero-cache/src/observability/metrics.ts`:
 *
 *     getOrCreateCounter('sync', 'foo', 'description')  ->  zero.sync.foo
 *
 * so a declaration is a call to one of those helpers with a literal category and
 * name, plus a description that may be a concatenation or a template. Some
 * declarations sit behind a one-hop local wrapper
 * (`litestreamDurationHistogram(name, description)`), which is expanded from its
 * call sites. `anonymous-otel-start.ts` talks to the OTel meter directly, so
 * those calls are recognised too.
 *
 * NOTHING IS DROPPED SILENTLY. A recognised call site that resolves to neither a
 * literal name nor a wrapper expansion lands in `unresolved`, which the CLI
 * prints and the views report. A quietly skipped instrument would show up as a
 * package that exports less than it does, which is a lie the graph should never
 * tell on its own.
 */

import {readFileSync, readdirSync} from 'node:fs';
import {join, relative} from 'node:path';
import {parseSync} from 'oxc-parser';
import {
  compareText,
  REPO_ROOT,
  type AttributeConfidence,
  type MetricFamily,
  type MetricType,
} from './model.ts';

/** The weaker reading wins: a series is only fully known if every site was. */
function mergeConfidence(
  a: AttributeConfidence,
  b: AttributeConfidence,
): AttributeConfidence {
  if (a === 'partial' || b === 'partial') return 'partial';
  if (a === 'complete' || b === 'complete') return 'complete';
  return 'unseen';
}

/**
 * The helper layer. Each takes `(category, name, descriptionOrOptions)` and
 * publishes the series as `zero.<category>.<name>`.
 */
const HELPERS = new Map<string, MetricType>([
  ['getOrCreateCounter', 'counter'],
  ['getOrCreateUpDownCounter', 'updowncounter'],
  ['getOrCreateGauge', 'gauge'],
  ['getOrCreateHistogram', 'histogram'],
  ['getOrCreateValueHistogram', 'histogram'],
  ['getOrCreateNativeHistogram', 'histogram'],
  ['getOrCreateLatencyHistogram', 'histogram'],
]);

/**
 * Direct OTel meter calls, which take the full series name. Only
 * `anonymous-otel-start.ts` uses these today.
 */
const METER_METHODS = new Map<string, MetricType>([
  ['createCounter', 'counter'],
  ['createUpDownCounter', 'updowncounter'],
  ['createObservableCounter', 'counter'],
  ['createObservableUpDownCounter', 'updowncounter'],
  ['createObservableGauge', 'gauge'],
  ['createGauge', 'gauge'],
  ['createHistogram', 'histogram'],
]);

/** Methods that record an observation, where attribute keys are visible. */
const RECORD_METHODS = new Set(['add', 'record', 'recordMs', 'observe']);

/** Cheap prefilter so most of the workspace is never parsed. */
const CANDIDATE =
  /getOrCreate[A-Z]|\.create(Counter|UpDownCounter|Observable\w+|Gauge|Histogram)\s*\(/;

const SKIP_DIRS = new Set([
  'node_modules',
  'out',
  'dist',
  'build',
  '.next',
  'docs',
]);
const SKIP_FILE = /\.(test|bench|spec)\.[cm]?tsx?$/;
const SOURCE_FILE = /\.[cm]?tsx?$/;

export type UnresolvedSite = {
  readonly file: string;
  readonly line: number;
  readonly via: string;
};

export type MetricsExtraction = {
  /** Package dir (repo-relative) -> the families declared under it. */
  readonly byDir: Map<string, MetricFamily[]>;
  readonly unresolved: UnresolvedSite[];
};

type AnyNode = {
  type: string;
  start: number;
  end: number;
  [key: string]: unknown;
};

function walk(node: unknown, visit: (node: AnyNode) => void): void {
  if (!node || typeof node !== 'object') return;
  if (Array.isArray(node)) {
    for (const child of node) walk(child, visit);
    return;
  }
  const record = node as Record<string, unknown>;
  if (typeof record.type === 'string') visit(record as AnyNode);
  for (const key of Object.keys(record)) {
    if (key === 'type' || key === 'start' || key === 'end') continue;
    walk(record[key], visit);
  }
}

function* sourceFiles(dir: string): Generator<string> {
  let entries;
  try {
    entries = readdirSync(dir, {withFileTypes: true});
  } catch {
    return;
  }
  for (const entry of entries) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      if (!SKIP_DIRS.has(entry.name) && !entry.name.startsWith('.')) {
        yield* sourceFiles(path);
      }
    } else if (SOURCE_FILE.test(entry.name) && !SKIP_FILE.test(entry.name)) {
      yield path;
    }
  }
}

/** Line number (1-based) of a byte offset, without splitting the whole file. */
function lineAt(source: string, offset: number): number {
  let line = 1;
  for (let i = 0; i < offset && i < source.length; i++) {
    if (source.charCodeAt(i) === 10) line++;
  }
  return line;
}

/**
 * Everything a metric declaration needs from one file, resolved against the
 * bindings available in it.
 */
function extractFile(
  absolute: string,
  source: string,
  families: MetricFamily[],
  unresolved: UnresolvedSite[],
): void {
  const file = relative(REPO_ROOT, absolute).replaceAll('\\', '/');
  const {program, errors} = parseSync(absolute, source);
  if (errors.length) {
    throw new Error(`${file}: ${errors[0].message}`);
  }

  // Module-level `const X = 'literal'`, so a description that splices one in
  // still resolves.
  const moduleConsts = new Map<string, string>();
  for (const statement of (program as unknown as {body: AnyNode[]}).body) {
    const decl = (
      statement.type === 'ExportNamedDeclaration'
        ? statement.declaration
        : statement
    ) as AnyNode | null;
    if (!decl || decl.type !== 'VariableDeclaration' || decl.kind !== 'const') {
      continue;
    }
    for (const d of decl.declarations as AnyNode[]) {
      const id = d.id as AnyNode | undefined;
      const init = d.init as AnyNode | undefined;
      if (
        id?.type === 'Identifier' &&
        init?.type === 'Literal' &&
        typeof init.value === 'string'
      ) {
        moduleConsts.set(id.name as string, init.value);
      }
    }
  }

  type Bindings = Map<string, AnyNode | undefined> | null;

  /** Fold a node down to a string: literal, template, concatenation, or const. */
  function asString(
    node: AnyNode | undefined,
    bind: Bindings,
  ): string | undefined {
    if (!node) return undefined;
    if (node.type === 'Literal') {
      return typeof node.value === 'string' ? node.value : undefined;
    }
    if (node.type === 'TemplateLiteral') {
      const expressions = node.expressions as AnyNode[];
      if (expressions.length) return undefined;
      return (node.quasis as {value: {cooked: string}}[])
        .map(q => q.value.cooked)
        .join('');
    }
    if (node.type === 'BinaryExpression' && node.operator === '+') {
      const left = asString(node.left as AnyNode, bind);
      const right = asString(node.right as AnyNode, bind);
      return left === undefined || right === undefined
        ? undefined
        : left + right;
    }
    if (node.type === 'Identifier') {
      const name = node.name as string;
      if (bind?.has(name)) return asString(bind.get(name), null);
      return moduleConsts.get(name);
    }
    return undefined;
  }

  /** The options argument, which may be a bare description string. */
  function options(
    node: AnyNode | undefined,
    bind: Bindings,
  ): {description: string | undefined; unit: string | undefined} {
    if (!node) return {description: undefined, unit: undefined};
    const bare = asString(node, bind);
    if (bare !== undefined) return {description: bare, unit: undefined};
    if (node.type === 'Identifier' && bind?.has(node.name as string)) {
      return options(bind.get(node.name as string), null);
    }
    if (node.type !== 'ObjectExpression') {
      return {description: undefined, unit: undefined};
    }
    let description: string | undefined;
    let unit: string | undefined;
    for (const property of node.properties as AnyNode[]) {
      if (property.type !== 'Property') continue;
      const key = property.key as AnyNode;
      const name = (key.name ?? key.value) as string | undefined;
      if (name === 'description') {
        description = asString(property.value as AnyNode, bind);
      } else if (name === 'unit') {
        unit = asString(property.value as AnyNode, bind);
      }
    }
    return {description, unit};
  }

  // ---- local one-hop wrappers ---------------------------------------------
  // `function litestreamDurationHistogram(name, description) { return
  //  getOrCreateHistogram('replica', name, {description, ...}); }`
  type Wrapper = {
    type: MetricType;
    params: (string | null)[];
    args: (AnyNode | undefined)[];
    call: AnyNode;
  };
  const wrappers = new Map<string, Wrapper>();
  walk(program, node => {
    if (node.type !== 'FunctionDeclaration' || !node.id) return;
    const body = (node.body as {body: AnyNode[]} | undefined)?.body;
    if (!body || body.length !== 1) return;
    const statement = body[0];
    if (statement.type !== 'ReturnStatement') return;
    const call = statement.argument as AnyNode | undefined;
    if (!call || call.type !== 'CallExpression') return;
    const callee = call.callee as AnyNode;
    if (callee.type !== 'Identifier') return;
    const type = HELPERS.get(callee.name as string);
    if (!type) return;
    wrappers.set((node.id as AnyNode).name as string, {
      type,
      params: (node.params as AnyNode[]).map(p =>
        p.type === 'Identifier' ? (p.name as string) : null,
      ),
      args: call.arguments as AnyNode[],
      call,
    });
  });

  // ---- attribute keys, observed at record sites ---------------------------
  // Attributes are passed when an observation is recorded, not when the
  // instrument is declared, so they are collected by binding name within this
  // file. Best effort by construction: a computed key whose constant lives in
  // another module cannot be folded, and a record site in a different file is
  // not seen at all. The views say so rather than printing "none".
  // `const attributes = {...}` anywhere in the file, so a record site that
  // passes a prepared bag still yields its keys. Same-name bindings in
  // different scopes would collide; in practice these are one per file.
  const objectConsts = new Map<string, AnyNode>();
  walk(program, node => {
    if (node.type === 'VariableDeclarator') {
      const id = node.id as AnyNode | undefined;
      const init = node.init as AnyNode | undefined;
      if (id?.type === 'Identifier' && init?.type === 'ObjectExpression') {
        objectConsts.set(id.name as string, init);
      }
      return;
    }
    // `function fooAttrs(x) { return {a: x.a, b: x.b}; }` -- an attribute
    // builder. Its KEYS are what matters here, so the arguments never need
    // substituting.
    if (node.type !== 'FunctionDeclaration' || !node.id) return;
    const body = (node.body as {body: AnyNode[]} | undefined)?.body;
    if (!body || body.length !== 1 || body[0].type !== 'ReturnStatement') {
      return;
    }
    const returned = body[0].argument as AnyNode | undefined;
    if (returned?.type === 'ObjectExpression') {
      objectConsts.set((node.id as AnyNode).name as string, returned);
    }
  });

  type Observed = {keys: Set<string>; sites: number; opaque: number};

  function bagFor(node: AnyNode | undefined): AnyNode | undefined {
    if (!node) return undefined;
    if (node.type === 'ObjectExpression') return node;
    if (node.type === 'Identifier') {
      return objectConsts.get(node.name as string);
    }
    if (node.type === 'CallExpression') {
      const fn = node.callee as AnyNode;
      if (fn.type === 'Identifier') return objectConsts.get(fn.name as string);
    }
    return undefined;
  }

  function readBag(observed: Observed, bag: AnyNode): void {
    for (const property of bag.properties as AnyNode[]) {
      if (property.type !== 'Property') {
        // A spread pulls in keys from somewhere this pass cannot follow.
        observed.opaque++;
        continue;
      }
      const key = property.key as AnyNode;
      if (property.computed) {
        const folded = asString(key, null);
        if (folded === undefined) observed.opaque++;
        else observed.keys.add(folded);
        continue;
      }
      const name = (key.name ?? key.value) as string | undefined;
      if (name) observed.keys.add(name);
      else observed.opaque++;
    }
  }

  // An observable gauge reports through a callback chained straight onto its
  // declaration -- `getOrCreateGauge(...).addCallback(r => r.observe(v, {k}))`.
  // The callback is right there, so these read exactly, without needing to know
  // what the instrument was stored as.
  const attributesByDeclaration = new Map<AnyNode, Observed>();
  walk(program, node => {
    if (node.type !== 'CallExpression') return;
    const callee = node.callee as AnyNode;
    if (callee.type !== 'MemberExpression') return;
    if ((callee.property as AnyNode | undefined)?.name !== 'addCallback') {
      return;
    }
    const declaration = callee.object as AnyNode;
    if (declaration.type !== 'CallExpression') return;

    const observed: Observed = {keys: new Set(), sites: 0, opaque: 0};
    walk((node.arguments as AnyNode[])[0], inner => {
      if (inner.type !== 'CallExpression') return;
      const innerCallee = inner.callee as AnyNode;
      if (innerCallee.type !== 'MemberExpression') return;
      if ((innerCallee.property as AnyNode | undefined)?.name !== 'observe') {
        return;
      }
      observed.sites++;
      // Either `observe(value, attrs)` or the batch form
      // `observe(instrument, value, attrs)`, so take the first bag after the
      // leading value argument.
      const args = (inner.arguments as AnyNode[]).slice(1);
      const bag = args.map(bagFor).find(Boolean);
      if (bag) readBag(observed, bag);
      else if (args.length) observed.opaque++;
    });
    if (observed.sites) attributesByDeclaration.set(declaration, observed);
  });

  const attributesByBinding = new Map<string, Observed>();
  walk(program, node => {
    if (node.type !== 'CallExpression') return;
    const callee = node.callee as AnyNode;
    if (callee.type !== 'MemberExpression') return;
    const method = (callee.property as AnyNode | undefined)?.name as
      | string
      | undefined;
    if (!method || !RECORD_METHODS.has(method)) return;
    // Either a stored instrument (`this.#foo.add(...)`) or one fetched inline
    // (`initialSyncRuns().add(...)`, `getOrCreateCounter(...).add(...)`), which
    // is how the factory-function style in this repo reads.
    const receiver = callee.object as AnyNode;
    const target = bindingName(receiver);
    const declaration = target ? undefined : declarationFor(receiver);
    if (!target && !declaration) return;

    let observed = target
      ? attributesByBinding.get(target)
      : attributesByDeclaration.get(declaration!);
    if (!observed) {
      observed = {keys: new Set(), sites: 0, opaque: 0};
      if (target) attributesByBinding.set(target, observed);
      else attributesByDeclaration.set(declaration!, observed);
    }
    observed.sites++;

    // `add(1)` / `record(v)` carries no attributes at all, which is a real
    // reading rather than a gap. The attribute bag is the argument after the
    // value.
    const args = (node.arguments as AnyNode[]).slice(1);
    if (!args.length) return; // `add(1)` -- undimensioned, and that is a reading
    const bag = args.map(bagFor).find(Boolean);
    if (bag) readBag(observed, bag);
    else observed.opaque++;
  });

  /** The declaration node an inline `factory().add(...)` receiver denotes. */
  function declarationFor(node: AnyNode): AnyNode | undefined {
    if (node.type !== 'CallExpression') return undefined;
    const callee = node.callee as AnyNode;
    if (callee.type !== 'Identifier') return undefined;
    const name = callee.name as string;
    if (HELPERS.has(name)) return node;
    return wrappers.get(name)?.call;
  }

  /** `foo`, `this.#foo`, `this.foo` -> a stable key for the instrument. */
  function bindingName(node: AnyNode | undefined): string | null {
    if (!node) return null;
    if (node.type === 'Identifier') return node.name as string;
    if (node.type === 'PrivateIdentifier') return `#${node.name as string}`;
    if (node.type === 'MemberExpression') {
      const object = node.object as AnyNode;
      if (object.type !== 'ThisExpression') return null;
      const property = node.property as AnyNode;
      if (property.type === 'PrivateIdentifier') {
        return `#${property.name as string}`;
      }
      if (property.type === 'Identifier') return property.name as string;
    }
    return null;
  }

  /** The name a declaration's result is stored under, if any. */
  function declaredAs(call: AnyNode): string | null {
    // Walk up is not available on this AST, so the enclosing declaration is
    // found by scanning declarations whose initialiser span contains the call.
    let best: string | null = null;
    let bestSpan = Infinity;
    walk(program, node => {
      let name: string | null = null;
      let value: AnyNode | undefined;
      if (node.type === 'VariableDeclarator') {
        name = bindingName(node.id as AnyNode);
        value = node.init as AnyNode | undefined;
      } else if (node.type === 'PropertyDefinition') {
        name = bindingName(node.key as AnyNode);
        value = node.value as AnyNode | undefined;
      } else if (node.type === 'AssignmentExpression') {
        name = bindingName(node.left as AnyNode);
        value = node.right as AnyNode | undefined;
      }
      if (!name || !value) return;
      if (call.start < value.start || call.end > value.end) return;
      const span = value.end - value.start;
      if (span < bestSpan) {
        bestSpan = span;
        best = name;
      }
    });
    return best;
  }

  function push(
    type: MetricType,
    category: string | null,
    name: string,
    optionsNode: AnyNode | undefined,
    bind: Bindings,
    site: AnyNode,
  ): void {
    const {description, unit} = options(optionsNode, bind);
    const binding = declaredAs(site);
    const observed =
      attributesByDeclaration.get(site) ??
      (binding ? attributesByBinding.get(binding) : undefined);
    families.push({
      name: category === null ? name : `zero.${category}.${name}`,
      type,
      category,
      unit: unit ?? null,
      description: description ?? null,
      attributes: observed ? [...observed.keys].toSorted(compareText) : [],
      attributeConfidence:
        !observed || observed.sites === 0
          ? 'unseen'
          : observed.opaque > 0
            ? 'partial'
            : 'complete',
      file,
      line: lineAt(source, site.start),
    });
  }

  // The helper layer itself calls the OTel meter with a computed name -- that is
  // the factory, not a declaration. A file that defines one of the helpers is
  // recognised as that layer, so the rule follows the code if it ever moves.
  let isHelperModule = false;
  walk(program, node => {
    if (node.type !== 'FunctionDeclaration') return;
    const id = node.id as AnyNode | undefined;
    if (id?.type === 'Identifier' && HELPERS.has(id.name as string)) {
      isHelperModule = true;
    }
  });

  // ---- declaration sites ---------------------------------------------------
  const wrapperCalls = new Set(Array.from(wrappers.values(), w => w.call));
  const expandedWrapperBodies = new Set<AnyNode>();

  const sites: AnyNode[] = [];
  walk(program, node => {
    if (node.type === 'CallExpression') sites.push(node);
  });

  for (const site of sites) {
    const callee = site.callee as AnyNode;
    const args = site.arguments as AnyNode[];

    if (callee.type === 'Identifier' && HELPERS.has(callee.name as string)) {
      const type = HELPERS.get(callee.name as string)!;
      const category = asString(args[0], null);
      const name = asString(args[1], null);
      if (category !== undefined && name !== undefined) {
        // Resolves on its own — including a wrapper body that hard-codes both,
        // which is a complete declaration wherever it is called from.
        push(type, category, name, args[2], null, site);
      } else if (wrapperCalls.has(site)) {
        // Needs its caller's arguments; recorded once per call site below.
        expandedWrapperBodies.add(site);
      } else {
        unresolved.push({
          file,
          line: lineAt(source, site.start),
          via: callee.name as string,
        });
      }
      continue;
    }

    if (callee.type === 'Identifier' && wrappers.has(callee.name as string)) {
      const wrapper = wrappers.get(callee.name as string)!;
      const bind = new Map<string, AnyNode | undefined>();
      wrapper.params.forEach((param, i) => {
        if (param) bind.set(param, args[i]);
      });
      const category = asString(wrapper.args[0], bind);
      const name = asString(wrapper.args[1], bind);
      if (category === undefined || name === undefined) {
        unresolved.push({
          file,
          line: lineAt(source, site.start),
          via: callee.name as string,
        });
        continue;
      }
      // Only wrappers that needed expansion contribute here; one that resolved
      // on its own was already recorded and must not be double-counted.
      if (expandedWrapperBodies.has(wrapper.call) || !literalWrapper(wrapper)) {
        push(wrapper.type, category, name, wrapper.args[2], bind, wrapper.call);
      }
      continue;
    }

    if (callee.type === 'MemberExpression') {
      const method = (callee.property as AnyNode | undefined)?.name as
        | string
        | undefined;
      const type = method ? METER_METHODS.get(method) : undefined;
      if (!type || isHelperModule) continue;
      const name = asString(args[0], null);
      if (name === undefined) {
        unresolved.push({file, line: lineAt(source, site.start), via: method!});
        continue;
      }
      push(type, null, name, args[1], null, site);
    }
  }

  function literalWrapper(wrapper: Wrapper): boolean {
    return (
      asString(wrapper.args[0], null) !== undefined &&
      asString(wrapper.args[1], null) !== undefined
    );
  }
}

/**
 * Scan the given package directories for metric declarations.
 *
 * `dirs` is repo-relative, longest first wins when a file could belong to more
 * than one (it cannot today, but nesting a package inside another is legal).
 */
export function extractMetrics(dirs: readonly string[]): MetricsExtraction {
  const ordered = dirs.toSorted((a, b) => b.length - a.length);
  const byDir = new Map<string, MetricFamily[]>();
  const unresolved: UnresolvedSite[] = [];

  for (const dir of ordered) {
    const families: MetricFamily[] = [];
    for (const absolute of sourceFiles(join(REPO_ROOT, dir))) {
      const source = readFileSync(absolute, 'utf8');
      if (!CANDIDATE.test(source)) continue;
      extractFile(absolute, source, families, unresolved);
    }
    if (families.length) {
      // One series can be declared at more than one site (the helper layer
      // caches by name, so the first call wins at runtime); keep one entry and
      // union what each site could tell us.
      const merged = new Map<string, MetricFamily>();
      for (const family of families.sort(
        (a, b) => compareText(a.file, b.file) || a.line - b.line,
      )) {
        const previous = merged.get(family.name);
        if (!previous) {
          merged.set(family.name, family);
          continue;
        }
        merged.set(family.name, {
          ...previous,
          description: previous.description ?? family.description,
          unit: previous.unit ?? family.unit,
          attributes: [
            ...new Set([...previous.attributes, ...family.attributes]),
          ].toSorted(compareText),
          attributeConfidence: mergeConfidence(
            previous.attributeConfidence,
            family.attributeConfidence,
          ),
        });
      }
      byDir.set(
        dir,
        [...merged.values()].toSorted((a, b) => compareText(a.name, b.name)),
      );
    }
  }

  unresolved.sort((a, b) => compareText(a.file, b.file) || a.line - b.line);
  return {byDir, unresolved};
}
