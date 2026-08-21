/**
 * Flags log calls that pass app data.
 *
 * At a log sink the useful question is "what is the static type of this
 * expression", not "where did this value come from" -- so this reads types from
 * the TypeScript checker rather than doing dataflow analysis. It also unwraps
 * "laundering": a value that becomes a `string` via `JSON.stringify(...)` or a
 * template span hides its type at the sink, so those are followed inward.
 *
 * What it cannot see: data baked into an `Error` message at construction and
 * logged from a different function. That needs real interprocedural analysis.
 *
 * Suppress a reviewed site with a comment on the line or the line above:
 *
 *   // log-allow: pg_replication_slots rows, not customer data
 *   lc.info?.('dropped slots', {dropped});
 */
import {readFileSync} from 'node:fs';
import {dirname, join, resolve} from 'node:path';
import {argv, exit, stdout} from 'node:process';
import {Node, Project, SyntaxKind} from 'ts-morph';

/** Repo root, so package paths resolve the same from any cwd. */
const ROOT = resolve(dirname(import.meta.dirname), '../..');

const LOG_METHODS = new Set(['info', 'debug', 'warn', 'error']);

/**
 * Types whose values are customer app data. `postgres.Row` is included on
 * purpose: a query against a customer table returns those. The ones that are
 * infrastructure metadata get suppressed individually, so the default stays
 * fail-safe.
 */
const APP_DATA_TYPES = [
  'Row',
  'RowList',
  'JWTPayload',
  'AST',
  'Condition',
  'LiteralValue',
  'CRUDOp',
  'InsertOp',
  'UpdateOp',
  'DeleteOp',
  'UpsertOp',
  'Mutation',
  'ClientSchema',
  'Notice',
  'PostgresError',
];

const TYPE_RE = new RegExp(`\\b(${APP_DATA_TYPES.join('|')})\\b`, 'g');

/** Names of app-data types appearing anywhere in a printed type, nested included. */
function matchedTypes(typeText: string): string[] {
  return [...new Set(typeText.match(TYPE_RE) ?? [])];
}

/** Printed types can be enormous; keep a readable preview. */
function preview(typeText: string): string {
  const cleaned = typeText
    .replace(/import\("[^"]*"\)\./g, '')
    .replace(/\s+/g, ' ');
  return cleaned.length > 72 ? `${cleaned.slice(0, 72)}...` : cleaned;
}
const SUPPRESS_RE = /\blog-allow:\s*(.+)$/;
const SKIP_FILE_RE = /\.(test|bench)\.tsx?$/;

type Finding = {
  file: string;
  line: number;
  column: number;
  via: string;
  expr: string;
  matched: string[];
  type: string;
};

function parseArgs(args: string[]) {
  const json = args.includes('--json');
  const paths = args.filter(a => !a.startsWith('--'));
  return {json, paths: paths.length ? paths : ['packages/zero-cache']};
}

/** True when `line` or the line above carries a `log-allow:` comment. */
function isSuppressed(lines: string[], lineIndex: number): boolean {
  for (const i of [lineIndex, lineIndex - 1]) {
    const text = lines[i];
    if (text !== undefined && SUPPRESS_RE.test(text)) return true;
  }
  return false;
}

function isLogCall(node: Node): boolean {
  if (!Node.isCallExpression(node)) return false;
  const callee = node.getExpression();
  return (
    Node.isPropertyAccessExpression(callee) && LOG_METHODS.has(callee.getName())
  );
}

/**
 * Collects the expressions whose type actually determines what gets printed,
 * following up to `MAX_DEPTH` hops of stringify/interpolation.
 */
const MAX_DEPTH = 3;

function collectExpressions(node: Node): {expr: Node; via: string}[] {
  const out: {expr: Node; via: string}[] = [];

  const walk = (expr: Node, via: string, depth: number): void => {
    if (depth > MAX_DEPTH) return;

    if (Node.isTemplateExpression(expr)) {
      for (const span of expr.getTemplateSpans()) {
        walk(span.getExpression(), 'interp', depth + 1);
      }
      return;
    }

    if (Node.isCallExpression(expr)) {
      const name = expr.getExpression().getText();
      if (name === 'JSON.stringify' || name === 'stringify') {
        for (const arg of expr.getArguments()) {
          walk(arg, 'stringify', depth + 1);
        }
        return;
      }
    }

    out.push({expr, via});
  };

  (node as never as {getArguments(): Node[]})
    .getArguments()
    .forEach((arg, i) => walk(arg, `arg${i}`, 0));

  return out;
}

function main() {
  const {json, paths} = parseArgs(argv.slice(2));
  const findings: Finding[] = [];
  let sites = 0;
  let suppressed = 0;

  for (const pkg of paths) {
    const pkgDir = resolve(ROOT, pkg);
    const project = new Project({
      tsConfigFilePath: join(pkgDir, 'tsconfig.json'),
    });

    for (const file of project.getSourceFiles()) {
      const path = file.getFilePath();
      if (path.includes('/node_modules/') || SKIP_FILE_RE.test(path)) {
        continue;
      }
      if (!path.startsWith(`${pkgDir}/`)) continue;

      let lines: string[] | undefined;

      for (const call of file.getDescendantsOfKind(SyntaxKind.CallExpression)) {
        if (!isLogCall(call)) continue;
        sites++;

        for (const {expr, via} of collectExpressions(call)) {
          let typeText: string;
          try {
            typeText = expr.getType().getText(expr);
          } catch {
            continue;
          }
          const matched = matchedTypes(typeText);
          if (matched.length === 0) continue;

          const {line, column} = file.getLineAndColumnAtPos(expr.getStart());
          lines ??= readFileSync(path, 'utf8').split('\n');
          if (isSuppressed(lines, line - 1)) {
            suppressed++;
            continue;
          }

          findings.push({
            file: path.replace(`${ROOT}/`, ''),
            line,
            column,
            via,
            expr: expr.getText().replace(/\s+/g, ' ').slice(0, 60),
            matched,
            type: preview(typeText),
          });
        }
      }
    }
  }

  if (json) {
    stdout.write(`${JSON.stringify({sites, suppressed, findings}, null, 2)}\n`);
  } else {
    for (const f of findings) {
      stdout.write(
        `${f.file}:${f.line}:${f.column}\n` +
          `  [${f.via}] ${f.expr}\n` +
          `  app data: ${f.matched.join(', ')}\n` +
          `  type: ${f.type}\n\n`,
      );
    }
    stdout.write(
      `${sites} log sites scanned, ${findings.length} flagged` +
        `${suppressed ? `, ${suppressed} suppressed` : ''}\n`,
    );
    if (findings.length) {
      stdout.write(
        `\nEach flagged argument prints a value whose static type is app data.\n` +
          `Log an identifier, a count or a shape instead -- or, if the value is\n` +
          `safe here, add a "// log-allow: <reason>" comment above the line.\n`,
      );
    }
  }

  exit(findings.length ? 1 : 0);
}

main();
