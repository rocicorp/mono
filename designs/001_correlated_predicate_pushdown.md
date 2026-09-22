# 001: Correlated predicate pushdown

- **Status:** Implemented (core scope; extensions remain under Future work)
- **Date:** 2026-09-21
- **Packages:** `zql` (the rewrite), `zero-cache` (kill switch)

## Summary

When a parent query pins a correlation column, for example
`user.where('user_id', X)`, every child row that can join to it has the same
value in the matching child column. We copy that condition into the child
subquery: `reading.where('user_id', X)`. This does not change the result.
It changes the cost of a push. A push that climbs the tree fetches parents
with SQL that now includes the ancestor's condition, so it no longer reads
every row that shares a join key with the changed row. The pushed condition
is also a filter on the child's connection. So a change that cannot match it
stops at the source, and indexes over connection filters can use it.

The rewrite is an AST pass inside `buildPipeline`. It is on by default, on
the client and the server. zero-cache gets a flag that turns it off.

## Problem

Take this query. Assume `user.user_id → reading.user_id`,
`reading.work_id → works.id` and `works.id → covers.work_id`.

```ts
user
  .where('user_id', X)
  .related('reading', r => r.related('works', w => w.related('covers')));
```

The pipeline is:

```
source(user, user_id = X) ─ Join(reading)
                              └ source(reading) ─ Join(works)
                                                    └ source(works) ─ Join(covers)
                                                                        └ source(covers)
```

This is what happens when a `covers` row with `work_id = W` is added:

1. `Join(covers)` fetches `works WHERE id = W`. This returns one row.
2. `Join(works)` fetches `reading WHERE work_id = W`
   (`Join.#pushChildChange` in `packages/zql/src/ivm/join.ts`). This returns
   every reading of W, for all users. Each one becomes a `child` change.
3. For each of those readings, `Join(reading)` fetches
   `user WHERE user_id = r.user_id AND user_id = X`. All but one return
   nothing.

The cost is O(readers of W) row reads, parent lookups, and change objects.
That cost repeats for each open query that has this shape. A popular work
with many readers and many open queries makes the cost multiply. This is a
runaway push.

The condition that makes step 2 small is known when the query is built.
Only readings with `user_id = X` can reach the output. But that condition
is on the `user` connection. It is not on the `reading` connection, and the
step-2 fetch reads from the `reading` connection.

### The same shape in zbugs

`issueDetail` with `idField = 'id'` is
`issue.where('id', I).related(...)`:

- **`comments` → `creator`.** A change to user U fetches every comment
  that U wrote, on every issue. `comments` has `limit(50)`, so the fetch
  goes through `Take` without a partition constraint (the `maxBound` path in
  `packages/zql/src/ivm/take.ts`). That path scans U's comments and checks
  the take state of each one.
- **`labels`** (`issue → issueLabel → label`). A label rename fetches every
  `issueLabel` row for that label.
- **`emoji` → `creator`.** A change to user U fetches every emoji that U
  created.

Each of these happens once for each open issue page.

`take.ts` has a comment that names this problem: "We could remove this case
if we added a translation layer (powered by some state) in join."

## Goals and non-goals

Goals:

- Remove this fan-out when an ancestor pins the correlation column with a
  literal.
- Make no change to query results, to the synced rows, or to the wire
  protocol.
- Make no change to the planner's decisions in v1.

Non-goals (see [Future work](#future-work)):

- Fan-out when the ancestor's condition is on a column that no relationship
  correlates on, as in the query below.
- Deriving facts about the parent from its EXISTS children.

```ts
// A change to a user still reads all of that user's comments.
issue
  .where('projectID', P)
  .limit(20)
  .related('comments', c => c.related('creator'));
```

## Design

### The rule

Each edge from a parent to a child has a correlation
`parentField[i] ↔ childField[i]`. The edge is either a `.related()`
subquery or an EXISTS / NOT EXISTS in `where`.

1. **Collect the facts at the edge.** These are the conditions that are
   true for every parent row where the edge can affect the result.
   - For `.related()`: the top-level AND conjuncts of `parent.where`.
   - For an EXISTS or NOT EXISTS in `where`: the conjuncts of every AND on
     the path from the root of `where` to the subquery condition. An OR on
     that path adds no facts. Facts from above the OR still apply.
2. **Keep the pushable facts.** In v1, a fact is pushable when all of these
   are true:
   - It is a simple condition with a column on the left and a literal on
     the right.
   - The column is in `parentField`.
   - The operator is `=`, `IS`, or `IN`, and an `IN` list has at most
     `MAX_PUSHED_IN_VALUES` values.
   - The parent column and the child column have the same Zero type, and
     that type is not `json`.
3. **Rename and add.** Change the column from `parentField[i]` to
   `childField[i]`. AND the renamed conditions into `child.where` and run
   `simplifyCondition`.
4. **Recurse** into the child with its new `where`. This moves facts down a
   chain when each hop correlates on the same column. For example, user →
   reading → a table keyed on `user_id`.

Sketch:

```ts
export function pushDownCorrelatedPredicates(
  ast: AST,
  columnsOf: (table: string) => Record<string, SchemaValue>,
): AST {
  function visit(ast: AST): AST {
    const where = ast.where && visitCondition(ast.where, ast.table, []);
    const facts = topLevelSimpleConjuncts(where);
    return {
      ...ast,
      where,
      related: ast.related?.map(csq => ({
        ...csq,
        subquery: visit(pushInto(csq, facts, ast.table)),
      })),
    };
  }

  function visitCondition(
    c: Condition,
    table: string,
    facts: readonly SimpleCondition[],
  ): Condition {
    switch (c.type) {
      case 'simple':
        return c;
      case 'and': {
        const inner = [...facts, ...c.conditions.filter(isSimple)];
        return {
          ...c,
          conditions: c.conditions.map(x => visitCondition(x, table, inner)),
        };
      }
      case 'or':
        return {
          ...c,
          conditions: c.conditions.map(x => visitCondition(x, table, facts)),
        };
      case 'correlatedSubquery':
        return {
          ...c,
          related: {
            ...c.related,
            subquery: visit(pushInto(c.related, facts, table)),
          },
        };
    }
  }

  // pushInto: filter `facts` to the pushable ones (step 2), rename them to
  // the child columns, and AND them into `csq.subquery.where` (step 3).
  // Returns the subquery unchanged when nothing is pushable.

  return visit(ast);
}
```

The pass makes new objects and does not change its input. The spreads keep
the `flip` flags that `planQuery` set.

### Examples

The problem query:

```ts
// Written
user
  .where('user_id', X)
  .related('reading', r => r.related('works', w => w.related('covers')));

// Built
user
  .where('user_id', X)
  .related('reading', r =>
    r.where('user_id', X).related('works', w => w.related('covers')),
  );
```

`works` gets nothing, because `reading → works` correlates on `work_id` and
the facts about `reading` are about `user_id`.

zbugs `issueDetail` with `idField = 'id'`:

- `comments` gets `issueID = I`.
- `issueLabel` (the hidden junction) gets `issueID = I`.
- `emoji` gets `subjectID = I`.

With `idField = 'shortID'` nothing is pushed, because `shortID` is not a
correlation column.

An EXISTS:

```ts
issue
  .where('projectID', P)
  .whereExists('project', p => p.where('visibility', 'public'));
// The project subquery gets `id = P`.
```

Nothing is pushed in these cases:

```ts
user.where('name', 'bob').related('reading'); // `name` is not correlated
user
  .where(({or, cmp}) => or(cmp('user_id', X), cmp('role', 'admin')))
  .related('reading'); // not a conjunct leaf
```

### Why the rewrite is sound

A `Join` matches a parent row p to a child row c when `valuesEqual` is true
for each pair of correlated columns (`packages/zql/src/ivm/data.ts`). That
function returns false for null and otherwise uses `===`. So for each joined
pair, `c[childField[i]] === p[parentField[i]]`, and neither value is null.
Let the fact F be `a op lit` and let F′ be the renamed `b op lit`. On a
joined pair, F′(c) evaluates the same test on the same value as F(p).

SQL agrees. For `=`, `IS`, and `IN`, a parent p passes F only when `p[a]`
equals the literal, or one of the listed literals. The join then fetches
p's children with the constraint `b = p[a]`. So F′ compares the child column
against the same value that the join's constraint already binds. Filter
literals are bound by their JS type, and SQLite compares them using the
column's type affinity. Step 2 requires equal Zero types, which keeps the
pass away from column pairs where the two comparisons could differ.

- **`.related()`.** Every output parent p satisfies F. Adding F′ to the
  child removes only child rows with ¬F′(c). Those rows cannot join to any
  output parent. The relationship of each output parent does not change.
- **EXISTS and NOT EXISTS.** F is a conjunct of every AND that encloses the
  subquery condition. So for any p where ¬F(p), an enclosing AND is already
  false, and the value of the subquery condition does not matter. For any p
  where F(p), the set of correlated child rows does not change, so the
  subquery condition has the same value.
- **Child `limit`.** `Take` and `Cap` partition by `childField`. The
  partition for p's key already contains only rows that satisfy F′.
- **Recursion.** After the rewrite, the child returns the same rows for
  every parent that matters. The same argument then applies one level down,
  with the child's new `where` as the source of facts.

The argument does not depend on how the operators work inside. The
rewritten AST is a different query with the same result, and the IVM
operators are correct for any query they get. The synced rows are the same
too. The one difference: for EXISTS, `Cap` does not order its input, so it
can keep a different set of up to 3 matching rows (1 for permission
subqueries). Any matching row is a valid witness for the client.

### Where the pass runs

In `buildPipeline` (`packages/zql/src/builder/builder.ts`), after
`completeOrdering` and after `planQuery`:

```ts
ast = delegate.mapAst ? delegate.mapAst(ast) : ast;
ast = completeOrdering(ast, ...);
if (costModel) {
  ast = planQuery(ast, costModel, planDebugger, lc);
}
if (!delegate.disableCorrelatedPredicatePushdown) {
  ast = pushDownCorrelatedPredicates(
    ast,
    table => must(delegate.getSource(table)).tableSchema.columns,
  );
}
return buildPipelineInternal(ast, delegate, queryID, '');
```

At this point:

- The AST is final. On the server, read permissions are applied and static
  parameters are bound (`read-authorizer.ts`). `pipeline-driver.ts` has
  already turned `{scalar: true}` subqueries into literal conditions
  (`resolveSimpleScalarSubqueries`), so the pass can push those literals
  too.
- `mapAst` has run, so column names match the sources.
- The same code runs on the client (`query-delegate-base.ts`) and on the
  server (`pipeline-driver.ts`, `run-ast.ts`, `write-authorizer.ts`).

#### Why after `planQuery`

If the pass ran before the planner, the planner would see the cheaper
child. But the planner's model reads a pushed condition on the correlation
column as a selective filter, and in the semi-join direction it is not
selective:

- `PlannerConnection` computes `selectivity` as rows with filters divided by
  rows without filters, with no constraint. A pushed `user_id = X` on
  `reading` gives a selectivity of about 1 / (number of users).
- `PlannerJoin` uses `1 - (1 - child.selectivity) ^ fanout` as the chance
  that a parent passes the EXISTS. With the pushed condition this number
  is close to zero. But for a parent that reaches the join, the pushed
  condition is always true.
- When a join constraint binds the same column, the SQLite cost model sees
  `user_id = ? AND user_id = ?`. It can lower its row estimate for the
  second term.

These errors push the planner toward flipping a pinned child, and that plan
may often be a good one. But the numbers are wrong, and a wrong model is
hard to reason about. In v1 the pass runs after planning, so the planner makes
the same decisions as today. The pushdown only removes rows from fetches in
the plan that the planner selected. A planner that knows about pushed
conditions is listed in [Future work](#future-work).

#### Other places we rejected

- **The query builder (`.related()`).** The parent's `where` is not final
  at that point, because `user.related(...).where(...)` is legal. The pass
  would also change the AST on the wire and the query hashes, and it would
  not see server-side permissions.
- **The planner.** It runs only on the server, and only when
  `enableQueryPlanner` is on. This rewrite is always correct, so it does not
  belong in a cost-based search.
- **At run time through `req.filter`.** `Join(works)` fetches from the
  `reading` pipeline. It does not know the `user` filter unless we add
  wiring and a new operator contract. The static rewrite puts the condition
  on the `reading` source connection, where the existing filter path already
  works.

### Effect on execution

The pushed condition goes into the child's source connection. The
connection's filter is compiled into the SQL for every fetch
(`TableSource.#requestToSQL`), and the source applies it to pushes.

| Path                                               | Before                                                           | After                                                    |
| -------------------------------------------------- | ---------------------------------------------------------------- | -------------------------------------------------------- |
| Upward fetch from a grandchild push (step 2 above) | `reading WHERE work_id = ?`                                      | `reading WHERE work_id = ? AND user_id = ?`              |
| Push of a `reading` row for another user           | Goes to `Join(reading)`, which looks up `user` and finds nothing | The source drops it                                      |
| Hydration fetch of `reading` for parent X          | `WHERE user_id = ?`                                              | `WHERE user_id = ? AND user_id = ?`: redundant but cheap |
| Flipped EXISTS child (the planner chose to flip)   | All child rows that match the child's own `where`                | Also filtered by the pushed condition                    |
| `Take`, no partition constraint (`maxBound` path)  | Reads rows from all partitions, up to `maxBound`                 | Reads only rows that pass the pushed condition           |

On the client, `MemorySource` applies the filter in JS. Client fan-out is
limited to rows that the client has synced, so the gain there is smaller.
The client runs the pass too, so the builder has one code path.

### Push-time filtering and connection indexing

The pushed condition is also a push-time filter. Take
`posts.where('id', 42).related('comments')`, with the correlation
`posts.id → comments.post_id`.

Today the correlation reaches the `comments` connection only as a fetch
constraint: `Join` fetches `comments WHERE post_id = 42`. `source.connect`
gets only the subquery's own `where`, and that is empty. So the `comments`
connection has no filter, and every `comments` change is pushed into it.

With the pass, the `comments` subquery has `where: post_id = 42` when the
builder calls `source.connect`. It is an ordinary connection filter, the
same as one the user wrote:

- **Pushes stop at the source.** `genPushAndWriteWithSplitEdit` runs
  `filterPush` with the connection's predicate. A change to a comment on
  another post stops there. Today it reaches `Join(comments)`, which fetches
  `posts WHERE id = c.post_id AND id = 42` and finds nothing.
- **Indexes over connection filters can use it.** Take an index that sends a
  source change only to the connections, or client groups, whose equality
  filters can match it. It cannot skip a connection that has no filter. A
  `.related()` with no `where` of its own is such a connection. If most
  client groups hold a query with one on a table, the index can skip almost
  nothing for that table. With the pass, the connection gets an equality
  filter on the join column whenever the parent pins that column.

The rule's limits apply. The parent must pin the exact column that the
relationship correlates on. The chain stops at the first hop whose
correlation column is not pinned. For example, if a `works` subquery gets
`user_id = X` and correlates to a catalog table on `work_id`, the catalog
connection gets nothing. Tables past that hop need the runtime approach
(Future work, item 5).

### What v1 pushes and why

- **`=` and `IS`.** Their gains come from index use. `IS NULL` is sound
  too: a join never matches null, so the child becomes empty, and the join
  already gives an empty child.
- **`IN` with a cap.** `IN` compiles to `IN (SELECT value FROM json_each(?))`
  (`packages/zqlite/src/query-builder.ts`). Each per-parent hydration fetch
  of the child carries the whole list. k parents cost O(k²) list work. v1
  caps the list at `MAX_PUSHED_IN_VALUES`. Start with 32. Constraint
  folding (see Future work) removes the need for the cap.
- **Literal right-hand sides only.** Static parameters are bound before
  `buildPipeline` on the server, and client ASTs do not contain them. Only
  literals let us check the size of an `IN` list.
- **Leaf conditions only.** `or(a = 1, a = 2)` is sound but rare, because
  people write `IN`. Pushing only leaves makes it easy to mark pushed
  conditions later for the planner.
- **Equal column types, not `json`.** SQLite compares using column
  affinity, and the JS join uses `===`. Requiring equal Zero types keeps
  the pass away from column pairs where these could disagree. Equal Zero
  types can still hide different Postgres types (for example `text` and
  `uuid`). For `=`, `IS`, and `IN`, the pushed condition binds the same
  value against the child column that the join constraint already binds,
  so it cannot add a new mismatch.
- **Range, `LIKE`, `!=`, `NOT IN`.** These are sound, but gain little. Not
  in v1.

### Kill switch

- **zql.** Add an optional field to `BuilderDelegate`:
  `disableCorrelatedPredicatePushdown?: boolean | undefined`. When it is
  `undefined`, the pass runs. The client never sets it.
- **zero-cache.** Add `enableCorrelatedPredicatePushdown` to `zero-config.ts`
  as a hidden option, `v.boolean().default(true)`. It is read from
  `ZERO_ENABLE_CORRELATED_PREDICATE_PUSHDOWN`. `PipelineDriver` already gets
  `config`. It sets the delegate field on both of its `buildPipeline` calls
  (hydration and the scalar subquery executor). `analyze.ts` / `run-ast.ts`
  and `write-authorizer.ts` read the same flag, so that `analyze` output
  matches production. The help-text snapshot in `zero-config.test.ts`
  must be updated.

The client has no switch. Client correctness depends on the tests below.
If the pass has a bug, a fix needs a client release.

## Risks

- **A different SQLite plan for the child.** A new predicate can make
  SQLite pick a different index. In hydration the predicate is on the
  column that the constraint already binds, so the best index does not
  change. On upward fetches, each of the two predicates is selective. We
  check this with `EXPLAIN QUERY PLAN` for the zbugs queries.
- **More statement shapes.** The statement cache is keyed by SQL text. Each
  pushed shape adds one entry for each query shape, not for each value,
  because values are bound as parameters.
- **Not all the fan-out is removed.** For `emoji` under `comments` in
  zbugs, a change to a user still reads every emoji that user created. The
  correlation `comment.id → emoji.subjectID` is not pinned. Each of those
  rows now fails at the `comments` fetch, one level earlier than before.
  The runtime semi-join in Future work fixes this.
- **Correctness bugs in the pass.** The pass is small and pure, and the
  argument above is short. The fuzz suites below test it with random
  pushes.

## Testing and measurement

- **Unit tests for the pass** (`correlated-predicate-pushdown.test.ts`):
  - Top-level `=`, `IS`, and `IN` (under and over the cap) are pushed.
  - Conditions under an OR are not pushed. Facts from above an OR are
    pushed into EXISTS below it.
  - Compound correlations push only the columns that have facts.
  - Nested ANDs around EXISTS and NOT EXISTS.
  - Transitive chains.
  - Type mismatch and `json` are skipped.
  - Static parameters are skipped.
  - `flip` flags are kept, and the input AST does not change.
- **Pipeline tests** (`builder.test.ts` and the `fetch-and-push-tests`
  harness):
  - For the user → reading → works → covers shape, push a `covers` row.
    Assert that the `reading` fetch includes `user_id` and that the number
    of rows read does not depend on the number of readers.
  - For `posts.where('id', 42).related('comments')`, assert that the
    `comments` connection's filter contains `post_id = 42`. Assert that a
    push of a comment on another post does not reach `Join(comments)`.
  - Assert the same view output with the pass on and off.
- **Differential fuzzing.** The chinook suites in
  `packages/zql-integration-tests` (`chinook-fuzz-*`, `fuzz/push.ts`) check
  zqlite and memory IVM against Postgres after each push step. The pass is
  on by default, so they cover it. But today's generated filters seldom
  pin a correlation column. Add a root filter axis: `=` or `IN` on a column
  that an outgoing relationship correlates on.
- **Benchmark** (`packages/zql-benchmarks`). Build the four-level shape with
  N readers of one work. Measure one `covers` push with the pass on and
  off, for N = 10², 10⁴, 10⁶. Expected: before grows linearly with N, after
  stays constant.
- **zbugs.** Open `issueDetail` for an issue, then edit a user who has
  written many comments, and rename a common label. Compare the rows read
  per SQL statement (from `DebugDelegate.getVendedRowCounts`, as `analyze`
  shows them) with the pass on and off.

## Rollout

1. Land the pass in zql (always on) and the zero-cache flag (default on) in
   one release.
2. Watch push and advance times on the zbugs deployment. The inspector's
   `query-update-server` metric comes from `MeasurePushOperator`.
3. Remove the flag after it has been on for a release with no problems.

## Future work

1. **Planner-aware pushdown before `planQuery`.** Mark pushed conditions
   (leaves only, so the marks survive `transformFilters` and
   `simplifyCondition`). Leave marked conditions out of `selectivity`, and
   out of the cost call when the constraint binds their column. Keep them
   when the child is the outer loop. Then the pass can run before planning,
   and the planner can choose flips that the pushdown makes cheap. For
   example, a flipped `user.where('user_id', X).whereExists('reading', …)`
   then fetches `reading WHERE user_id = X`.
2. **Constraint folding in `TableSource`.** When `req.constraint` binds
   every column of a filter conjunct, evaluate the conjunct once in JS. If
   it is true, leave it out of the SQL. If it is false, return no rows and
   do not run the query. This removes the redundant hydration predicate for
   pushed conditions and for user-written ones, and removes the need for
   the `IN` cap.
3. **Facts from EXISTS.** For EXISTS (not NOT EXISTS), a child fact on
   `childField` is also a fact about the parent:
   `whereExists('owner', o => o.where('id', U))` implies `ownerID = U`.
   This is sound and cheap. People rarely write it, but permission rules
   can. It can be part of the same pass.
4. **More operators.** ORs of pushable leaves, range operators, and `LIKE`.
5. **Runtime semi-join reduction.** A static fact is a bound, known at
   build time, on the set of parent keys that are in the view. The real
   bound is that set. `Join` could keep a refcounted set of the parent key
   values in the view, and upward fetches in the child could add
   `childField IN (live set)` through `multiConstraints`. That removes
   fan-out when the ancestor's condition is not on a correlation column,
   and under `limit` windows. It costs memory for each join, updates on
   each push, and care with the overlay logic. This is the "translation
   layer (powered by some state) in join" that the `take.ts` comment
   describes.
6. **Pinning through a unique key.** `issue.where('shortID', N)` pins one
   row, but not through `id`. We could resolve it to `id = …` at build
   time, with a companion subquery as scalar subqueries use. Then
   `issueDetail` with `idField = 'shortID'` would get the pushdown.

## Relation to existing work

- **#5894 (`req.filter`).** That change pushes a filter down to its own
  source, in the same pipeline and on the same table (`FilterStart`).
  `packages/zql/src/ivm/predicate-pushdown.test.ts` tests it. This design
  copies a filter across a join to a different table. To keep the two
  separate, this one is called _correlated_ predicate pushdown.
- **`FlippedJoin.fetch`.** It already translates a `req.constraint` on
  parent join keys into a child constraint at run time. It does not see the
  parent's connection filter. This pass supplies that filter statically.
