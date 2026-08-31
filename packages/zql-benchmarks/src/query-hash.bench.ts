/**
 * Benchmarks for `hashOfAST` (`packages/zero-protocol/src/query-hash.ts`).
 *
 * `hashOfAST(ast)` is `h64(JSON.stringify(normalizeAST(ast))).toString(36)`.
 * `h64` used to run `xxHash32` twice over the string, and `xxHash32` UTF-8
 * encodes its string argument on every call, so one hash allocated a JSON
 * string plus two identical byte arrays and walked the bytes twice. `h64` now
 * encodes once and derives both words in one pass; `multiPassH64` below is the
 * old behaviour, kept as the "before" baseline.
 *
 * Both `normalizeAST` and `hashOfAST` memoize on object identity, so the
 * pipeline only runs for an AST the process has not seen before. That is the
 * common case in a client: a component that rebuilds its query on render
 * produces a fresh AST object each time. These benchmarks therefore measure the
 * uncached pipeline, feeding it an already-normalized AST so the numbers are
 * about hashing and not about `normalizeAST`.
 *
 * The `parts` suite breaks the pipeline down; the `pipeline` suite compares the
 * whole thing before and after.
 */
import {xxHash32} from 'js-xxhash';
import {bench, describe, use} from '../../shared/src/bench.ts';
import {h64} from '../../shared/src/hash.ts';
import {normalizeAST, type AST} from '../../zero-protocol/src/ast.ts';
import {hashOfAST} from '../../zero-protocol/src/query-hash.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../zql/src/query/query.ts';
import {builder} from './schema.ts';

function astOf(q: AnyQuery): AST {
  return asQueryInternals(q).ast;
}

// `SELECT * FROM user WHERE id = ? LIMIT 1`
const tiny = astOf(builder.user.where('id', 'user-1').one() as AnyQuery);

// Roughly the zbugs issue list.
const medium = astOf(
  builder.issue
    .where('open', true)
    .where('visibility', 'public')
    .related('labels')
    .related('creator')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(100) as AnyQuery,
);

// Roughly the zbugs `issuePreloadV2` query: relationships several levels deep.
const large = astOf(
  builder.issue
    .whereExists('project', q => q.where('id', 'project-zero'), {
      scalar: true,
    })
    .related('labels')
    .related('viewState', q => q.where('userID', 'user-1'))
    .related('creator')
    .related('assignee')
    .related('emoji', emoji => emoji.related('creator'))
    .related('comments', comments =>
      comments
        .related('creator')
        .related('emoji', emoji => emoji.related('creator'))
        .limit(10)
        .orderBy('created', 'desc'),
    )
    .where('visibility', 'public')
    .orderBy('modified', 'desc')
    .orderBy('id', 'desc')
    .limit(1000) as AnyQuery,
);

const CASES = [
  ['tiny', tiny],
  ['medium', medium],
  ['large', large],
] as const;

let encoder: TextEncoder | undefined;

/** What `h64` did before: one encode and one pass per 32-bit word. */
function multiPassH64(str: string): bigint {
  let hash = 0n;
  for (let i = 0; i < 2; i++) {
    hash = (hash << 32n) + BigInt(xxHash32(str, i));
  }
  return hash;
}

describe('hashOfAST parts', () => {
  for (const [name, ast] of CASES) {
    const normalized = normalizeAST(ast);
    const json = JSON.stringify(normalized);
    const bytes = new TextEncoder().encode(json);
    const big = h64(json);
    const label = `${name} (${json.length} chars)`;

    bench(`${label} | cache hit`, () => {
      use(hashOfAST(ast));
    });

    bench(`${label} | whole pipeline`, () => {
      use(h64(JSON.stringify(normalized)).toString(36));
    });

    bench(`${label} | whole pipeline, old h64`, () => {
      use(multiPassH64(JSON.stringify(normalized)).toString(36));
    });

    bench(`${label} | JSON.stringify`, () => {
      use(JSON.stringify(normalized));
    });

    bench(`${label} | TextEncoder.encode`, () => {
      use((encoder ??= new TextEncoder()).encode(json));
    });

    bench(`${label} | xxHash32(string)`, () => {
      use(xxHash32(json, 0));
    });

    bench(`${label} | xxHash32(bytes)`, () => {
      use(xxHash32(bytes, 0));
    });

    bench(`${label} | h64(string)`, () => {
      use(h64(json));
    });

    bench(`${label} | bigint.toString(36)`, () => {
      use(big.toString(36));
    });
  }
});

describe('hashOfAST pipeline', () => {
  for (const [name, ast] of CASES) {
    const normalized = normalizeAST(ast);
    const json = JSON.stringify(normalized);
    const label = `${name} (${json.length} chars)`;

    bench(`${label} | before: h64 encoded and walked twice`, () => {
      use(multiPassH64(JSON.stringify(normalized)).toString(36));
    });

    bench(`${label} | after: h64 encodes once, one pass`, () => {
      use(h64(JSON.stringify(normalized)).toString(36));
    });
  }
});
