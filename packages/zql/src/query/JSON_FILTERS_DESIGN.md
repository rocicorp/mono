# Design: JSON filters (and JSON in joins) — accessing JSON values in `where` clauses

**Issue:** [zbugs #3385](https://bugs.rocicorp.dev/p/zero/issue/3385) — _"Allow accessing
JSON values in `where` clauses."_

**Status:** Phase 1 (JSON filters) implemented; Phases 2–3 (JSON in declared
relationships; on-the-fly acceleration for ad-hoc `orderBy`) proposed.

### Implementation status

- **Done (Phase 1):** a dedicated `JsonPathReference` AST node wrapping a
  `ColumnReference` (AST + valita schema + normalize/hash + name-mapping maps the
  wrapped column and preserves the path); `eb.json(col, ...path)` accessor accepted
  by `cmp` (the chosen API — see §5.2); in-memory predicate navigation; SQLite
  `json_extract` pushdown; `PROTOCOL_VERSION` 51 → 52. Covered by unit tests
  (predicate, AST hash/mapping, `ast-to-zql` render) and an end-to-end zqlite test
  (builder → AST → `json_extract` → results).
- **Done (Phase 1, Postgres):** the `z2s` compiler emits `#>>` json/jsonb text
  extraction with the leaf and the literal cast to a common type derived from the
  literal (see §5.5); validated against live Postgres (`compiler.pg.test.ts`).
- **Done (Phase 1, typing):** `cmp`'s comparison value is typed from the JSON-path
  leaf, and `json()` path segments are validated/autocompleted against the column's
  declared type (`ValueAtPath` / `ValidJsonPath`; see §5.2). Untyped `json()` stays
  permissive (`ReadonlyJSONValue`).
- **Not started:** Phase 2 (relationships) and Phase 3 (orderBy / on-the-fly).

---

## 1. Goal & scope

Let a comparison reference a value _inside_ a `json()`-typed column, and (in a later
phase) let a declared relationship correlate on such a value.

```ts
// Filter on a JSON path:
z.query.issue.where(({cmp, json}) =>
  cmp(json('metadata', 'priority'), '=', 'high'),
);
z.query.issue.where(({cmp, json}) =>
  cmp(json('metadata', 'address', 'zip'), '=', '94110'),
);
```

### In scope

- **JSON paths in filter predicates** — `where` / `cmp`, every existing operator
  (`= != < > <= >= LIKE ILIKE IN NOT IN IS IS NOT`), including inside `exists` /
  related subqueries. Object-key and array-index segments. **Scalar leaves.**
- **JSON paths as declared relationship correlation keys** (Phase 2 — see §6) —
  `sourceField` / `destField` may point into JSON.

### Out of scope (initially)

- JSON paths in **`orderBy`** (per-query, dynamic — the only case that needs
  runtime materialization; see §7).
- Comparing two paths (a path on the **RHS** of a comparison).
- **Deep object/array** equality (only scalar leaves compared).

These are the cases that interact with joins/ordering; §5–§7 explain why they
split out the way they do.

---

## 2. Background: filtering & relationships before this change

This section is the **pre-change baseline** that motivated the design; line numbers
are approximate and predate the Phase 1 edits (e.g. a JSON path is now modeled by the
`JsonPathReference` node described in §3).

| Concern             | File                                                | Behavior                                                                                                                                                                                                                                            |
| ------------------- | --------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Condition AST       | `zero-protocol/src/ast.ts:67`                       | `ColumnReference = {type:'column', name:string}`. The comment already flags that `name` will _"need to be a path through the tree"_. `right` is `Exclude<ValuePosition, ColumnReference>` (RHS is never a column).                                  |
| Builder             | `zql/src/query/expression.ts:192`                   | `cmp` → `{type:'simple', left:{type:'column',name:field}, right, op}`.                                                                                                                                                                              |
| Types               | `zql/src/query/query.ts:21`                         | `NoCompoundTypeSelector` **excludes** `json` and array columns from `cmp`/`where` selectors — today you can't even name a json column.                                                                                                              |
| In-memory predicate | `zql/src/builder/filter.ts:87`                      | `createPredicate` reads `row[left.name]`, null-guards, applies a JS comparator. JSON columns are **already parsed JS objects in rows** (`zqlite/src/table-source.ts:647` `JSON.parse` on read; stored as `TEXT`, `JSON.stringify` on write `:597`). |
| SQLite compile      | `zqlite/src/query-builder.ts:226`                   | column → `sql.ident(name)`. RHS literal coerced by `toSQLiteType(v, getJsType(v))` — typed off the **literal's** JS type (bool→`1/0`).                                                                                                              |
| Postgres compile    | `z2s/src/compiler.ts:459`                           | column → `colIdent`; RHS literal cast using the **column's declared server type** (`sqlConvertColumnArg(getServerColumn(...))`).                                                                                                                    |
| Joins / PK lookups  | `zql/src/ivm/constraint.ts:13,147`                  | `Constraint = {[column]: Value}`; `extractColumn` keys off `left.name`; sources are kept **sorted/indexed by real columns**.                                                                                                                        |
| Relationships       | `zero-schema/src/builder/relationship-builder.ts:6` | Declared with `sourceField` / `destField` / `destSchema`.                                                                                                                                                                                           |
| Correlation AST     | `zero-protocol/src/ast.ts:245`                      | `Correlation = {parentField: CompoundKey, childField: CompoundKey}` — compound keys of **column names**, name-mapped at lowering (`transformAST` `key()`, `:364`).                                                                                  |
| Normalize / hash    | `ast.ts:531`, `query-hash.ts:12`                    | `compareValuePosition` compares column refs by `name` only; `hashOfAST = JSON.stringify(normalizeAST)`. `transformWhere` (`ast.ts:401`) rewrites client→server **column names**.                                                                    |

Two facts drive the whole design:

1. **JSON columns are already parsed JS objects in `Row`** on both client and server.
   A filter is therefore a pure per-row predicate — it needs no schema or storage
   change.
2. **The IVM correlates joins and maintains sort order by _named columns_.**
   `Row` is keyed by column name; `Constraint` is `{column: value}`; sources merge
   sorted streams by named columns. A JSON-extracted value is not such a column.

---

## 3. The core idea: a dedicated reference node that wraps a column

There are two distinct notions of "path", and they must stay orthogonal:

- a **JSON path** — navigation _within a single value_ (`issue.metadata.priority`);
- a **relationship path** — navigation _across tables_ (the `ColumnReference` TODO).
  This is handled by query nesting / correlation, not by JSON access.

We model JSON access as its own value-position node that **wraps** a
`ColumnReference` and carries the within-value path. `ColumnReference` stays exactly
as it was (a plain current-table column):

```ts
export type ColumnReference = {
  readonly type: 'column';
  readonly name: string;
};

export type JsonPathReference = {
  readonly type: 'json';
  /** The column whose JSON value is navigated. */
  readonly value: ColumnReference;
  /**
   * JSON navigation within the column's value, applied left-to-right.
   * Object keys (string) and array indices (number).
   */
  readonly path: readonly (string | number)[];
};
```

`JsonPathReference` is added to the `ValuePosition` union and is allowed **wherever a
`ColumnReference` is** — currently only the filter LHS (`SimpleCondition.left`). It
is excluded from `SimpleCondition.right` alongside `ColumnReference` (no path on the
RHS in v1).

Why a wrapper node rather than an optional `path` field on `ColumnReference`? It
keeps `ColumnReference` unchanged (a plain column is never ambiguous), and it scales
to richer expressions later: the wrapper is a natural place to widen the wrapped
`value` (e.g. to other expressions) without disturbing the column type or every
existing `'column'` switch arm. JSON access stays independent of any future
cross-table comparison feature (which would be a _different_ extension).

---

## 4. Why joins/relations are the tricky part — and why they're still tractable

A JSON path can be a **predicate** but not, by itself, a **key**:

- IVM joins correlate via `Constraint` (`{realColumn: value}`), sources are kept
  **sorted by real columns**, and `orderBy` needs a named sort key. A
  `json_extract(...)` result is none of those — so the in-memory operators above
  the source have nothing to merge or seek on, **even if the replica had a perfect
  index**.
- A **filter** sidesteps all of this: it's a pure per-row predicate (the `Filter`
  operator + a `WHERE json_extract(...)` scan). No sort order, no correlation key.

So the question for joins/sorts is: _how does the extracted value become a named,
sorted, addressable column?_ The answer differs by case, and the key realization is:

> **Joins are _declared_ in the schema, so the set of JSON join-keys is static and
> known at schema-apply time.** Ordering is supplied per query, so it is the only
> genuinely dynamic case.

That single distinction collapses most of the difficulty:

| Case                            | Dynamic?     | Needs materialized column?         | Where it's resolved                     |
| ------------------------------- | ------------ | ---------------------------------- | --------------------------------------- |
| **Filter** on JSON              | per query    | **No** — predicate only            | client + server, no schema change       |
| **Join** on JSON (relationship) | **declared** | Yes, but **bounded/deterministic** | at schema apply                         |
| **`orderBy`** on JSON           | per query    | Yes                                | runtime / on-the-fly (or explicit decl) |

---

## 5. Phase 1 — JSON filters

No schema or replica change; pure predicate + SQL pushdown.

### 5.1 AST

- Add `JsonPathReference` as in §3 and a `jsonPathReferenceSchema` (`ast.ts`) =
  `{type:'json', value: columnReferenceSchema, path: v.readonlyArray(v.union(v.string(), v.number()))}`;
  add it to the `conditionValueSchema` union (LHS only).
- `right` stays `Exclude<ValuePosition, ColumnReference | JsonPathReference>` (no path on
  RHS in v1).
- `compareValuePosition` (`ast.ts`): add a `'json'` case — compare the wrapped column
  then the `path` (stable, distinct normalization).
- `transformWhere` (`ast.ts`): the `'json'` case maps the wrapped column's name and
  passes `path` through. **Path segments are JSON data, not schema names → must NOT
  be name-mapped.** A regression test pins this.
- `hashOfAST`: free — `JSON.stringify` includes the whole `JsonPathReference`.

### 5.2 Public API / builder

A typed left-value accessor on the expression builder, mirroring how
`ParameterReference` brands an object with a symbol that yields the AST node:

```ts
where(({cmp, json}) => cmp(json('metadata', 'priority'), '=', 'high'));
```

- _Implemented:_ `json(column, ...path)` returns a branded `ColumnRef<TColumnType, P>`
  accepted by `cmp` in the left position. `column` is constrained to the table's JSON
  columns (`JsonSelectors`); the path is captured as a literal tuple (`const P`).
- _Leaf-type inference (implemented):_ `cmp`'s `ColumnRef` overloads drive the
  comparison value's type from the leaf at the path — `ValueAtPath<TColumnType, P>`
  fed through `GetFilterTypeFromTSType`. So `cmp(json('metadata','priority'),'=',x)`
  types `x` as the leaf type (e.g. `'high' | 'low'`) and rejects mismatches. Untyped
  `json()` degrades to `ReadonlyJSONValue`.
- _Path-segment validation/autocomplete (implemented — Tier 2):_ `json`'s path
  parameter is `...path: P & ValidJsonPath<TColumnType, P>`. `ValidJsonPath` maps the
  path tuple to a tuple whose element at each position is the keys valid _there_
  (`JsonKeysOf` of the type reached by the preceding segments) — so an invalid key
  fails to satisfy its position (and editors autocomplete the valid keys). Object
  keys, array indices (`number`), and "any segment" for untyped `json()` are allowed;
  a scalar leaf has no further segments. Beyond the supported depth, segments are
  left unconstrained.

  Three implementation constraints worth recording (each learned the hard way — the
  first two caused a ~360-error cascade across the query builder):
  1. `ValueAtPath`/`ValidJsonPath` must be **non-recursive** (unrolled to a fixed
     depth). A self-recursive conditional in `cmp`/`json`'s signature defeats
     TypeScript's structural comparison of `ExpressionBuilder`.
  2. The leaf type must be resolved over a **concrete** type, not a deferred schema
     lookup _in `cmp`_. So `json` resolves the column's TS type at its (concrete) call
     site and stores it in `ColumnRef`; `cmp` walks that. Because that puts a
     schema-derived type in `json`'s **return**, `ColumnRef`'s type parameters are
     made **bivariant** (a method-style phantom brand) so a concrete-schema
     `ColumnRef` stays assignable to a generic-schema one.
  3. "Untyped" (`ReadonlyJSONValue`) is detected as
     `NonNullable<ReadonlyJSONValue> extends NonNullable<T>` — comparing the non-null
     forms, so a nullable column doesn't defeat the check.

Alternatives considered: a proxy/fluent `eb.col.metadata.priority` (prettiest, but
heavy/fragile typing) and a dotted-string selector (ambiguous — dots in keys,
json-vs-relationship, no indices).

> **Decided (Q1):** the explicit `eb.json(col, ...path)` accessor with full typing —
> leaf-type inference for the comparison value (Tier 1) and path-segment
> validation/autocomplete (Tier 2).

### 5.3 In-memory evaluation (`zql/src/builder/filter.ts`)

A `readColumn(row, ref)` helper; in `createPredicate`, a `'json'` left ref navigates
into the column value before the existing null-guard + comparator. As implemented, a
path miss (missing key, or a null/undefined intermediate) coalesces to `null` — JSON
has no `undefined`, and this matches SQLite `json_extract` so `IS NULL` agrees on
client and replica (§5.6/§5.7):

```ts
function readColumn(row, ref: ColumnReference | JsonPathReference): Value {
  if (ref.type === 'column') return row[ref.name]; // plain reads unchanged
  let v: Value = row[ref.value.name];
  for (const seg of ref.path) {
    if (v === null || v === undefined) return null;
    v = (v as Record<string | number, Value>)[seg];
  }
  return v === undefined ? null : v;
}
```

No new comparator (reuses `createPredicateImpl`). `IS`/`IS NOT` handle null/missing;
`LIKE`/`ILIKE` work for free on string leaves.

**Correctness-critical:** `constraint.ts:extractColumn` extracts only `'column'`
refs; a `'json'` ref falls through to `undefined` — otherwise
`cmp(json('meta','x'),'=',v)` would be mis-extracted as a primary-key/constraint
lookup keyed by the whole column. The cost model must likewise treat path predicates
as **non-index-backed** (scan).

### 5.4 SQLite compile (`zqlite/src/query-builder.ts`)

`valuePositionToSQL` `'json'` case: emit
`json_extract(<ident>, '$.<seg>...')` (object key → `.key` with escaping; array
index → `[i]`). Use `json_extract` (not `->>`) for portability — `->>`/`->` need
SQLite 3.38+, `json_extract` is universal. RHS already coerces via
`toSQLiteType(v, getJsType(v))` keyed on the literal's JS type — exactly right for a
dynamically-typed leaf. `IN`:
`json_extract(col,'$.p') IN (SELECT value FROM json_each(?))`.

### 5.5 Postgres compile (`z2s/src/compiler.ts`)

Implemented uniformly via **text extraction + a literal-driven cast**, which
sidesteps jsonb-comparison quirks and reuses the existing literal binding. A
`JsonPathReference` only ever appears on the **left**, and the other side is always
a literal, so the symmetric `valueComparison`/`literalValueComparison` flow already
renders both sides — the two `'json'` cases just make them line up:

- `valueComparison` `'json'` (`jsonPathLeaf`): emit
  `("col" #>> ARRAY['a','b']::text[])`. `#>>` works on both `json` and `jsonb` and
  maps **both** a missing key and a JSON null to SQL NULL — matching SQLite
  `json_extract` and the in-memory predicate, so `IS NULL` agrees across all three.
  The leaf is then cast to a type derived from the literal (`pgCastTypeForJsonLeaf`:
  string → `text`, number → `double precision`, boolean → `boolean`).
- `literalValueComparison` `'json'`: render the literal by its **own** JS type
  (`sqlConvert{Singular,Plural}LiteralArg`), not the column's server type — so its
  cast matches the leaf's.

This yields, e.g., `(col #>> ARRAY['priority']::text[])::text = $n::text::text` for
equality, `::double precision > …` for numeric ordering, `… ILIKE …` for text
patterns, `… = ANY(ARRAY(…))` for `IN`, and `(col #>> …) IS NOT DISTINCT FROM NULL`
for `IS NULL` (leaf left uncast so missing/JSON-null both read as SQL NULL).
Snapshot-tested in `compiler.output.test.ts` and **executed against live Postgres**
in `compiler.pg.test.ts`.

> Note: this targets object/scalar `json`/`jsonb` columns. A `json<T[]>()` column
> backed by a native Postgres array (server type `text[]`, not json) is not a `#>>`
> target; array-typed json columns aren't a JSON-path filter use case.

### 5.6 Cross-engine parity (test this hardest)

Three evaluators must agree: JS `===`/`<`, SQLite `json_extract`, PG `jsonb`.

- **bool:** JS `true` ↔ SQLite `1` ↔ PG jsonb `true` — consistent with the
  encodings above.
- **null vs missing key:** JS distinguishes `undefined`(missing) from `null`(JSON
  null); SQLite `json_extract` → SQL `NULL` for _both_; PG `#>` → `NULL`(missing) vs
  `'null'::jsonb`(JSON null) **differ**. **Decision:** collapse JSON-null and missing
  to the same outcome (use `#>>`, which maps JSON null → SQL NULL like missing); both
  → non-match for value ops, match for `IS NULL`. Document it.
- **object/array leaf:** out of scope v1 (scalar leaves only).
- **mixed-type ordering:** engine-specific; document "compare within one JSON type."

The in-memory predicate now coalesces a path miss (missing key / null intermediate)
to `null` so `IS NULL` matches both a missing key and a JSON null — identical to the
SQLite `json_extract` pushdown. Plain (path-less) reads are unchanged.

### 5.7 Storage encoding (gotcha)

`json_extract` only works if the column is stored as **real JSON text**
(`json_type` = object/array), not a double-encoded JSON _string_. The production
replicator does the right thing: `liteValue` (`zero-cache/src/types/lite.ts:82`)
stores PG json/jsonb as raw JSON text. But the IVM `TableSource.push` path runs
values through `toSQLiteType` (which `JSON.stringify`s json columns), so callers of
`push` (tests, client writes) must pass **raw objects**, not pre-stringified
strings — otherwise the value is stored double-encoded and every `json_extract`
returns `NULL`. (Several existing zqlite test fixtures seed pre-stringified JSON;
the JSON-filter tests use a self-contained source seeded with objects.)

---

## 6. Phase 2 — JSON in declared relationships (joins)

Relationships lower to a static correlation:

```ts
// ast.ts:245
type Correlation = {parentField: CompoundKey; childField: CompoundKey};
```

`sourceField`/`destField` are authored in the schema, so the complete set of join
keys is **known at schema-apply time** — bounded and deterministic. This is the
opposite of the filter LHS (dynamic) and removes the "ad-hoc index explosion"
hazards (no runtime registry, refcount, or GC needed for joins).

### 6.1 Representation

A correlation key element goes from a bare column name to a column-or-JSON
reference — reusing the `JsonPathReference`-wraps-`ColumnReference` shape from the filter
LHS (this is the Phase-2 extension that widens "where a `JsonPathReference` is allowed"
beyond the filter LHS). The name-mapping in `transformAST`/`transformWhere` maps the
wrapped column and passes the path through unchanged (same rule as §5.1).

### 6.2 Materialization at schema apply

For each path-valued correlation field, zero-cache synthesizes, when it applies the
schema, a **generated column + index** on that table's replica:

```sql
ALTER TABLE issue ADD COLUMN "zero/metadata.authorId" TEXT
  GENERATED ALWAYS AS (json_extract(metadata, '$.authorId')) VIRTUAL;
CREATE INDEX ... ON issue ("zero/metadata.authorId");
```

- Parent side: so the value is addressable to form the `Constraint`.
- Child side: so it's an **indexed seek** target.
- `VIRTUAL` avoids a table rewrite (computed on read); the index build is an O(n)
  scan but happens once at schema apply, off the query hot path.
- Bounded, deterministic, and **validated up front** — fail fast at schema apply if
  it can't be backed, rather than silently degrading.
- Replica-local (not upstream); recorded with the schema and reconstructed
  deterministically on resync.

The planner rewrites the path-correlation to the synthesized column name; after
that it's an ordinary column to the `Constraint`/Join/flip machinery — **no IVM
changes**. The source already introspects replica indexes at runtime
(`zqlite/src/table-source.ts:552`, `pragma_index_list`); the `sqlite-cost-model`
must learn about the synthesized index to pick it.

### 6.3 Client asymmetry

The client IVM has no SQLite — rows are JS objects, no DDL. So a relationship that
correlates on a JSON path requires the client to **materialize the same derived
field** via a JS projection (a Map step that adds `row["zero/metadata.authorId"]`
on load/change), so client-side correlation matches the server. This is bounded
because it's declared — the client derives it deterministically from the schema.

Two-hop (junction) relationships simply materialize each hop's side; no special case.

---

## 7. Phase 3 — physical acceleration & ad-hoc ordering

Two related "generate on the fly" mechanisms, needed only for the genuinely dynamic
cases:

- **Filter acceleration (optional, server-only).** A bare _expression index_
  `CREATE INDEX ... ON t (json_extract(c,'$.p'))` turns a filter's hydration scan
  into an index seek. It does **not** surface the value as a named field, but
  filters don't need that — the value never enters the IVM as a key. Low risk,
  pure perf, the client is uninvolved.
- **`orderBy` on a JSON path (the only case that truly needs runtime
  materialization).** Ordering is supplied per query, so it can't be pre-declared.
  Options: (a) require an explicit **declared computed column** (predictable), or
  (b) **on-the-fly generated column + index** with a lifecycle: a registry of
  synthesized `(table, path)` artifacts, refcounted by active queries, GC'd/LRU'd
  when unused, with a hard cap to bound index count and write amplification, built
  async with a scan fallback until ready. The client must materialize the same
  derived field for client-side sorting.

### Index vs. generated column (the key distinction)

- **Filter speedup** → _expression index_ suffices (the value stays in SQL).
- **Join / sort on JSON** → _generated column_ (a named, addressable, indexable
  field) **plus** a matching client-side JS projection — because the IVM needs the
  value as a key, not merely as a physically-indexed expression.

This is, in effect, **auto-materialized computed columns**. A natural follow-on is to
expose **explicit, schema-declared computed columns** as a first-class feature
(usable in filters, joins, orderBy, and select), with on-the-fly creation as the
optimizing layer over the same "materialize a derived field + index it" core.

---

## 8. Touch-point checklist

| #   | File                                              | Change                                                                                                                                                                                                                                                                                     |
| --- | ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1   | `zero-protocol/src/ast.ts`                        | `JsonPathReference` + `jsonPathReferenceSchema` (in `ValuePosition`/`conditionValueSchema`); `compareValuePosition` `'json'` case; correlation key element (Phase 2); `transformWhere`/`transformAST` map the wrapped column and pass `path` through unmapped. **(protocol version bump)** |
| 2   | `zql/src/query/expression.ts`                     | `json()` accessor + `ColumnRef`; `cmp`/`cmpLit` accept it.                                                                                                                                                                                                                                 |
| 3   | `zql/src/query/query.ts` + `expression.ts`        | typing: `JsonSelectors`, `ValueAtPath` (leaf), `ValidJsonPath`/`JsonKeysOf` (path validation), `GetFilterTypeFromTSType`; `cmp`/`json` overloads. **(done)**                                                                                                                               |
| 4   | `zql/src/builder/filter.ts`                       | path navigation in `createPredicate`.                                                                                                                                                                                                                                                      |
| 5   | `zql/src/ivm/constraint.ts`                       | `extractColumn` extracts only `'column'` refs; `'json'` refs fall through to `undefined`. **(correctness-critical)**                                                                                                                                                                       |
| 6   | `zqlite/src/query-builder.ts`                     | `json_extract` in `valuePositionToSQL`.                                                                                                                                                                                                                                                    |
| 7   | `z2s/src/compiler.ts`                             | `#>>` text extraction (`jsonPathLeaf`) + literal-driven cast (`pgCastTypeForJsonLeaf`); literal rendered by its own type. **(done; live-PG tested)**                                                                                                                                       |
| 8   | `ast-to-zql/src/ast-to-zql.ts`                    | render the path (inspector/debug).                                                                                                                                                                                                                                                         |
| 9   | `zero-cache .../analyze` (cost model)             | path predicate = scan, never a seek.                                                                                                                                                                                                                                                       |
| 10  | `zero-schema/src/builder/relationship-builder.ts` | accept a path in `sourceField`/`destField` (Phase 2).                                                                                                                                                                                                                                      |
| 11  | zero-cache schema-apply + replicator              | synthesize generated column + index for path correlations; record for resync (Phase 2).                                                                                                                                                                                                    |

---

## 9. Testing

- **Unit:** `createPredicate` path nav (nested object, array index, missing, null,
  bool/number/string, `IN`, `IS NULL`).
- **Snapshot:** zqlite (`json_extract` SQL) and z2s (`#>>` SQL); plus live-PG
  execution for z2s (`compiler.pg.test.ts`).
- **Parity (most important):** extend `zql-integration-tests` so the _same_ query
  yields identical results across client-IVM / zqlite / Postgres (the planner-exec
  harness already runs this comparison).
- **AST:** round-trip via `ast-to-zql`; hash distinctness; normalize ordering; pin
  that `path` survives name-mapping unmapped.
- **Types:** typed `json<T>()` inference; untyped `json()` fallback.
- **Phase 2:** schema-apply synthesizes the generated column + index; client
  projection matches; join results identical client vs server; resync rebuilds the
  artifact.

---

## 10. Rollout / protocol

Adding the `JsonPathReference` node (and later to correlation keys) is additive but
valita parse is **strict** (`shared/src/valita.ts:180`) → an older `zero-cache`
rejects an AST carrying a `'json'` ref. Gate behind a **protocol/AST version bump**;
a new client must not emit a `JsonPathReference` to an older server.

Done in Phase 1: `PROTOCOL_VERSION` bumped 51 → 52 and the `astSchema` guard hash
in `ast.test.ts` (plus the `protocol-version.test.ts` schema hash) updated.
`MIN_SERVER_SUPPORTED_SYNC_PROTOCOL` is unchanged (30), so clients only emit a
`JsonPathReference` once both ends are on ≥ 52, per the standard server-before-clients
deploy contract.

---

## 11. Open questions

Resolved during Phase 1:

- **API shape (Q1):** explicit `eb.json(col, ...path)` accessor. ✅
- **Array-index segments:** included in Phase 1 (segments are `string | number`). ✅
- **null/missing semantics:** collapse JSON-null and missing-key (§5.6); confirm this
  is the desired behavior. (Implemented this way for client/SQLite parity.)
- **path-on-RHS / path-to-path:** out for v1 (`right` stays
  `Exclude<ValuePosition, ColumnReference | JsonPathReference>`). ✅
- **Postgres (`z2s`) path support:** implemented via `#>>` text extraction + a
  literal-driven cast, validated against live Postgres (§5.5). ✅
- **Leaf-type inference for the comparison value** (`ValueAtPath`): implemented —
  the `cmp` value is typed from the path's leaf (§5.2). ✅
- **Path-segment validation/autocomplete** (`ValidJsonPath`): implemented — invalid
  path segments are rejected and valid keys autocomplete (§5.2). ✅

Still open:

1. **Computed columns**: implicit (on-the-fly) vs explicit (schema-declared) as the
   surface for ad-hoc `orderBy` — and do we want explicit computed columns as a
   first-class feature regardless?
