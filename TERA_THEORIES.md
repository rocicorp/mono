# Terabugs creator filter crash: analysis & diagnostic plan

## Context

The `issueListV2` query on terabugs (100M issues) crashes on the **live pipeline** (pipeline-driver) when a creator filter is applied via `whereExists('creator', q => q.where('login', creator), {scalar: true})`. Works fine without the filter.

## Code path analysis (live pipeline)

I traced the full flow through the code. **The code path looks correct** — no obvious bug:

1. **Scalar resolution works**: `pipeline-driver.ts:427` calls `#resolveScalarSubqueries()`. The `user.login` column has a unique index (`user_login_idx` from `0000_init.sql:95`). So `isSimpleSubquery()` returns true, and the creator scalar resolves to `creatorID = <value>` (a `SimpleCondition`).

2. **Cost model sees the condition**: `removeCorrelatedSubqueries()` in `sqlite-cost-model.ts:102` only strips `correlatedSubquery` type conditions. The resolved `creatorID = Y` is type `simple` — it passes through untouched.

3. **Planner doesn't interfere**: `processCondition()` in `planner-builder.ts:108-109` returns `input` for simple conditions (no-op). The condition is handled by the source SQL.

4. **SQL includes the filter**: `buildSelectQuery()` in `query-builder.ts:44-46` puts all filters into the WHERE clause. The SQL should be:

   ```sql
   SELECT ... FROM issue
   WHERE projectID = ? AND open = ? AND creatorID = ?
     AND (visibility = 'public' OR ...)
   ORDER BY modified DESC, id DESC
   ```

5. **Covering index exists**: `issue_projectID_open_creatorID_modified_idx` on `(projectID, open, creatorID, modified, id)` — perfect for this query.

## Theories (ranked by likelihood)

### Theory 1: SQLite stats missing → bad index choice

If `sqlite_stat1` doesn't have stats for the 5-column index, SQLite may pick `(projectID, open, modified, id)` instead (good for ORDER BY but requires scanning all `(projectID, open)` rows to filter by `creatorID`). On 100M rows with millions per project+open combo, this is a full scan.

**Why it could differ with/without creator**: Without creator filter, `(projectID, open, modified, id)` IS the right index. Adding `creatorID` requires the 5-column index, but SQLite only picks it with good stats.

**Diagnostic**: Run `SELECT * FROM sqlite_stat1 WHERE tbl = 'issue'` on the replica. Check if the 5-column index has stats.

### Theory 2: Start cursor OR + NULL interaction

When paginating with `.start()`, `gatherStartConstraints()` generates OR clauses with NULL checks:

```sql
(constraintValue IS NULL OR "modified" > constraintValue)
OR ("modified" IS constraintValue AND ...)
```

Per the AGENTS.md gotcha: NULL values in OR branches cause SQLite to abandon MULTI-INDEX OR optimization → full table scan. This only applies to paginated requests (not initial hydration).

**Diagnostic**: Check if the crash happens on initial load or only when paginating.

### Theory 3: Unexpected error/assertion (not performance)

The crash might not be a performance issue at all — could be an assertion failure, type error, or edge case in the pipeline construction. Without the stack trace, can't confirm.

**Diagnostic**: Get the actual error message / stack trace from the crash.

### Theory 4: Data distribution edge case

If the filtered creator has millions of issues (e.g., a bot), even with the right index the result set could be huge. With `LIMIT 1000`, the source should stop after 1000 rows, but if visibility filtering eliminates most rows (e.g., `visibility = 'public'` filters out 99%), the source might scan millions of index entries to find 1000 matching rows.

**Diagnostic**: Check how many issues the filtered creator has and what percentage pass the visibility filter.

## Diagnostic checklist (when dataset arrives)

1. **Get the crash error**: Stack trace, error message, OOM vs timeout vs assertion
2. **Check SQLite stats**: `SELECT * FROM sqlite_stat1 WHERE tbl = 'issue' ORDER BY idx`
3. **Run EXPLAIN QUERY PLAN** on the actual resolved SQL (with literal values inlined)
4. **Test initial load vs pagination**: Does it crash on first page or only when scrolling?
5. **Log the resolved AST**: Confirm `creatorID = <value>` is a simple condition (not still a correlated subquery)
6. **Check creator's issue count**: How many issues does the target creator have? Is it a bot with millions?
7. **Run ANALYZE on the SQLite replica**: If stats are missing, `ANALYZE` populates them and may fix the issue
8. **Compare EXPLAIN with/without creator**: See which index SQLite picks for each case

## If Theory 1 is confirmed (missing stats)

The fix would be to ensure `ANALYZE` runs on the SQLite replica after initial replication. This could be:

- A one-time `ANALYZE` after schema sync in zero-cache
- Periodic re-analyze after significant data changes
- Or: use `INDEXED BY` hints in the SQL (but this couples the query to specific index names)
