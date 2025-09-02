You're in a package that implements incremental view maintenance for queries defined via an AST. This IVM engine is    │
│   rather novel. It constructs a data flow graph as normal but also allows efficient "upqueries" to answer a query from   │
│   scratch. These "upqueries" also allow an operator to maintain minimal state as they can ask operators above them,      │
│   which in turn ask further operators, for the state they need.                                                          │
│                                                                                                                          │
│   `fetch` is the path used for an upquery. `push` is the path used when changes are made to the base database and the    │
│   query result needs to be incrementally updated.                                                                        │
│                                                                                                                          │
│   `fetch` is also used to hydrate a query when it is first run.                                                          │
│                                                                                                                          │
│   We currently have some issues with `whereExists`. This:                                                                │
│   1. creates a join node                                                                                                 │
│   2. creates an `exists` filter operator                                                                                 │
│                                                                                                                          │
│   The join order is the key issue. We need a way to put the most constrained, or smallest, table as the outer loop of    │
│   the join. Or, in other words, the first join node in the pipeline.                                                     │
│                                                                                                                          │
│   Our joins create a tree. Outputting a tree is a problematic structure when re-ordering joins as it changes the         │
│   output. Exist nodes, however, do not need to show in the final output.                                                 │
│                                                                                                                          │
│   Given that, my idea is to create a new `join` called `flatJoin` which creates a cartesian product rather than a tree.  │
│   This flat join would be used for exists.                                                                               │
│                                                                                                                          │
│   All flat joins would have to be processed first, followed by a new "distinct" operator to collapse back down to        │
│   single rows. We would be distincting on the primary keys of the root table that is driving the query. The root table   │
│   being the first table specified in the AST.                                                                            │
│                                                                                                                          │
│   Once we've distincted down, the rest of the pipeline and its join operators can run as normal.                         │
│                                                                                                                          │
│   Can you first create me a new operator for `distinct`? 



