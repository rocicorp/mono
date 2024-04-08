# ZQL

[./query/EntityQuery.ts](./query/EntityQuery.ts) is the main entrypoint for everything query related:

- building
- preparing
- running
- and materializing queries

# Creating an EntityQuery

First, build your schema for rails as you normally would:

```ts
const issueSchema = z.object({
  id: z.string(),
  title: z.string(),
  created: z.date(),
  status: z.enum(['active', 'inactive']),
});
type Issue = z.infer<typeof issueSchema>;
```

Then you can create a well-typed query builder

```ts
const query = new EntityQuery<Issue>(context, 'issue');
```

- The first param to `EntityQuery` is the integration point between the query builder and Replicache. It provides the query builder with a way to gain access to the current Replicache instance and collections. See [`makeTestContext`](./context/context.ts) for an example.
- The second param to `EntityQuery` is the same `prefix` parameter that is passed to rails `generate`. It is used to identify the collection being queried.

> Note: this'll eventually be folded into a method returned by [`GenerateResult`](../generate.ts) so users are not exposed to either parameter on `EntityQuery`.

# EntityQuery

[./query/EntityQuery.ts](./query/EntityQuery.ts)

`EntityQuery` holds all the various query methods and is responsible for building the AST ([`./ast/ZqlAst.ts`](./ast/ZqlAst.ts)) to represent the query.

Example:

```ts
const derivedQuery = query
  .where(...)
  .join(...)
  .select('id', 'title', 'joined.thing', ...)
  .asc(...);
```

Under the hood, `where`, `join`, `select`, etc. are all making a copy of and updating the internal `AST`.

Key points:

1. `EntityQuery` is immutable. Each method invoked on it returns a new query. This prevents queries that have been passed around from being modified out from under their users. This also makes it easy to fork queries that start from a common base.
2. `EntityQuery` is a 100% type safe interface to the user. Layers below `EntityQuery` which are internal to the framework do need to ditch type safety in a number of places but, since the interface is typed, we know the types coming in are correct.
3. The order in which methods are invoked on `EntityQuery` that return `this` does not and will not ever matter. All permutations will return the same AST and result in the same query.

Once a user has built a query they can turn it into a prepared statement.

# Prepared Statements

[./query/Statement.ts](./query/Statement.ts)

A prepared statement is used to:

1. Manage the lifetime of a query
2. In the future, change bindings of the query
3. De-duplicate queries
4. Materialize a query

```ts
const stmt = derivedQuery.prepare();
```

Lifetime - A statement will subscribe to its input sources when it is subscribed or when it is materialized into a view. For this reason, statements must be cleaned up by calling `destroy`.

Bindings - not yet implemented. See the ZQL design doc.

Query de-duplication - not yet implemented. See the ZQL design doc.

Materialization - the process of running the query and, optionally, keeping that query's results up to date. Materialization can be 1-shot or continually maintained.

# Prepared Statement Creation

[./ast-to-ivm/pipelineBuilder.ts](./ast-to-ivm/pipelineBuilder.ts)

When the user calls `query.prepare()` the `AST` held by the query is converted into a differential dataflow graph.

The resulting graph/pipeline is held by the prepared statement. The `pipelineBuilder` is responsible for performing this conversion.

[high level notes on dataflow](https://www.notion.so/replicache/Subscriptions-Differential-Data-Flow-18850074c1554c81b6f9ab7786e6cb8f)

The pipeline builder walks the AST --

1. When encountering tables (via `FROM` and `JOIN`) they are added as sources to the graph.
2. When encountering `JOIN`, adds a `JOIN` operator to join the two mentioned sources.
3. When encountering `WHERE` conditions, those are added as filters against the sources.
4. When encountering `SELECT` statements, those are added as `map` operations to re-shape the results.
5. `ORDER BY` and `LIMIT` are retained to either be passed to the source provider, view or both.

# Dataflow Internals: Source, DifferenceStream, Operator, View

Also see: [./ivm/README.md](./ivm/README.md)

The components, in code, that make up the dataflow graph are:

1. [ISource](./ivm/source/ISource.ts)
   1. [MemorySource](./ivm/source/MemorySource.ts)
   1. [StatelessSource](./ivm/source/StatelessSource.ts)
1. [DifferenceStream](./ivm/graph/DifferenceStream.ts)
   1. [DifferenceStreamReader](./ivm/graph/DifferenceStreamReader.ts)
   2. [DifferenceStreamWriter](./ivm/graph/DifferenceStreamWriter.ts)
1. [Operator](./ivm/graph/operators/Operator.ts)
   1. Join (handles `JOIN`)
   2. Map (handles `SELECT` as well as `function` application)
   3. Reduce (handles aggregates like `GroupBy`)
   4. Filter (handles `WHERE` and `ON` statements or `HAVING` when applied after a reduction)
   5. LinearCount (handles `COUNT`)
   6. Or
   7. And
   8. Union
   9. Intersect
1. [View](./ivm/view/IView.ts)
   1. ValueView
   2. TreeView

Conspicuously absent are `LIMIT` and `ORDER BY`. These are handled either by the sources or views. A future section is devoted to these two.

The above components would be composed into a graph like so:

![img](https://github.com/rocicorp/rails/assets/1009003/1809a63e-fcf9-4e3d-ac62-47f42e380024)

# Query Execution

Query execution, from scratch, and incremental maintenance are nearly identical processes.

The dataflow graph represents the execution plan for a query. To execute a query is a simple matter of sending all rows from all sources through the graph.

> See `Statement.test.ts` for examples of queries being run from scratch.

Execution can be optimized in the case where a `limit` and `cursor` are provided (not yet implemented here).

In other words, if:

1. the final view has the same ordering as the source and
2. the query specifies a cursor

We can jump to that set of rows rather than feeding all rows. If a limit is specified we can stop reading rows once we hit the limit.

The limit functionality is implemented by making `Multiset` lazy. See `Multiset.ts` for how this is currently implemented. The limited view that is pulling values from a multiset can stop without all values being visited.

Not yet implemented would be index selection. E.g., queries of the form:

```ts
Issue.where('id', '=', x);
Issue.where('id', '>', x);
```

should just be lookups against the primary key rather than a full scan.

If a view's order does not match a source's order, we will (not yet implemented here) create a new version of the source that is in the view's order. This source will be maintained in concert with the original source and used for any queries that need the given ordering.

# Incremental Maintenance

Incremental maintenance is simply a matter of feeding each write through the graph as it happens. The graph will produce the correct aggregate result at the end.

The details of _how_ that works are specific to individual operators. For operators that only use the current row, like map & filter, it is trivial. They just emit their result. For join and reduce (not implemented yet) it is more complex.

What has been implemented here lays the groundwork for join & reduce hence why it isn't as simple as a system that only needs to support map & filter.

# Sources: Stateful vs Stateless

Sources model tables. A source can come in stateful or stateless variants.

A stateless source cannot return historical data to queries that are subscribed after the source was created.

A stateful source knows it contents. When a data flow graph is attached to it, the full contents of the source is sent through the graph, effectively running the query against historical data.

# Views: ValueView, TreeView

There are currently two kinds of views: `ValueView` and `TreeView`.

`ValueView` maintains the result of a `count` query.

`TreeView` maintains `select` queries.

> count and select are distinct queries. This implementation does not support returning a count with other selected columns.

`TreeView` holds a comparator which uses the columns provided to `Order By` to sorts it contents. If no `Order By` is specified then the items are ordered by `id`. Any time an `Order By` is specified that does not include the `id`, the `id` is appended as the last item to order by. This is so we get a stable sort order when users sort on columns that are not unique.

Views can be subscribed to if a user wishes to be notified whenever the view is updated.

Aaron was pretty adamant about using native JS collections, hence the `value` property on `TreeView` returns a JS array. This is fine for cases where the view has a `limit` but for cases where you want a view of thousands+ of items I'd recommend the `PersistentTreapView` (not available here, but in Materialite).

# OR, Parenthesis & Breadth First vs Depth First Computation

This PR does not support `OR` or nested conditions but does lay the groundwork for it by executing the dataflow graph breadth fist rather than depth first.

This is the reason for the split of dataflow events between `enqueue` and `notify` or `run` and `notify` for operators.

See the commentary on `IOperator` in `Operator.ts`

# Transactions

The IVM system here has a concept of a transaction. It enables:

1. Many sources and values to be written before updating query results
2. Only notifying query subscribers after all queries have been updated with the results of the writes made in that transaction.

See commentary in `ISourceInternal` in `ISource.ts` as well as on `IOperator` in `Operator.ts`.
