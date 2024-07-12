The `ivm` directory implements incremental view maintenance for Zero.

In this context a “view” has the meaning from databases - an automatically
updated query result - not what it does in user interfaces.

The code in this directory allows users to build a “pipeline” which
incrementally maintains some ZQL query. The pipeline is a DAG. Nodes in the DAG
are either Sources, Operators, or Views. Edges in the DAG are DifferenceStreams.

Changes originate in the pipeline at Sources. These changes flow through
DifferenceStreams to the various Operators of the pipeline, getting transformed,
and are finally emitted at one or more Views.

Incrementally computing views via IVM is typically dramatically cheaper than
recomputing the query over and over. Moreover, the direct output of IVM is
precise changes to the view, which is useful (ie to send them from server to
client, or to update some UI).

The key classes and interfaces to understand to work with `ivm` are:

- [Multiset](./multiset.ts)
- [DifferenceStream](./graph/difference-stream.ts)
- [Source](./source/source.ts)
- [Operator](./graph/operators/operator.ts)
- [View](./view/view.ts)
