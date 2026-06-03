import {beforeAll, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {getLikePredicate} from '../../zql/src/builder/like.ts';
import {Database} from './db.ts';
import {format} from './internal/sql.ts';
import {filtersToSQL, type NoSubqueryCondition} from './query-builder.ts';

// Zero evaluates the same query two ways that must agree: the client-side IVM
// matcher (getLikePredicate, JS String.toLowerCase) and the zqlite replica
// (compiled to `lower(col) LIKE lower(pattern)`, backed by @rocicorp/zero-sqlite3's
// ICU lower()). This test pins that they produce identical ILIKE results across
// Unicode case-folding, wildcards, escapes and newlines.

function ivmIlike(pattern: string, input: string): boolean {
  return getLikePredicate(pattern, 'i')(input) as boolean;
}

function zqliteIlike(db: Database, pattern: string, input: string): boolean {
  // Use the real compiler output so we test the actual zqlite ILIKE SQL.
  const {text, values} = format(
    filtersToSQL({
      type: 'simple',
      left: {type: 'column', name: 'name'},
      op: 'ILIKE',
      right: {type: 'literal', value: pattern},
    } as NoSubqueryCondition),
  );
  const row = db
    .prepare(`SELECT (${text}) AS m FROM (SELECT ? AS name)`)
    .get<{m: number}>(...values, input);
  return !!row.m;
}

const cases: ReadonlyArray<readonly [pattern: string, input: string]> = [
  // Unicode case-insensitive equality (umlauts / accents / Cyrillic / Greek).
  ['müller', 'MÜLLER'],
  ['MÜLLER', 'müller'],
  ['café', 'CAFÉ'],
  ['привет', 'ПРИВЕТ'],
  ['σιγμα', 'ΣΙΓΜΑ'],
  ['müller', 'schmidt'], // no match
  ['å', 'Ä'], // distinct letters, no match

  // Wildcards over Unicode input.
  ['m%r', 'MÜLLER'],
  ['m_ller', 'müller'], // _ matches the single char ü
  ['%Ü%', 'müller'], // wildcard + case-insensitive fold
  ['x%', 'MÜLLER'], // no match

  // % and _ span newlines, matching SQLite LIKE.
  ['a%b', 'a\nb'],
  ['a_b', 'a\nb'],

  // Backslash escapes: \% and \_ are literal.
  [String.raw`100\%`, '100%'],
  [String.raw`100\%`, '100x'], // no match (literal %)
  [String.raw`a\_b`, 'a_b'],
  [String.raw`a\_b`, 'axb'], // no match (literal _)

  // ß is the case where folding (ß->ss) and lowering differ. BOTH backends
  // lower() rather than fold, so neither matches "ss" — and they still agree.
  ['straße', 'STRASSE'],
];

let db: Database;
beforeAll(() => {
  db = new Database(createSilentLogContext(), ':memory:');
});

test.for(cases)(
  'client-side and zqlite ILIKE agree: %j vs %j',
  ([pattern, input], {expect}) => {
    const ivm = ivmIlike(pattern, input);
    const sqlite = zqliteIlike(db, pattern, input);
    expect(sqlite, `ivm=${ivm} sqlite=${sqlite}`).toBe(ivm);
  },
);
