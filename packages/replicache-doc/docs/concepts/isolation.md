---
title: Database Isolation Level
slug: /concepts/db-isolation-level
---

**You should use [Snapshot Isolation](https://en.wikipedia.org/wiki/Snapshot_isolation) or better in your push and pull endpoints.**

:::tip

In PostgreSQL, this isolation level is confusingly called "[REPEATABLE READ](https://www.postgresql.org/docs/current/transaction-iso.html#XACT-REPEATABLE-READ)".

:::

This is often surprising to developers. Why does Replicache require such high level of consistency?

The Replicache push and pull endpoints each do several reads and writes and these operations need to get values that represent a consistent snapshot of the database.

For example, in the pull endpoint, we read the `lastMutationID` for one or more clients and also entity rows that have changed since the last pull. These values are returned to the client.

It's important that if the server says mutation `42` was processed, that the `patch` returned by the pull response must include the changes from mutation `42`. If it doesn't, then the user will see their data disappear: Replicache will remove the optimistic version of mutation `42` and its effects, but no authoritative version of the data from that mutation is present yet. The reverse can also happen and in that case, the user might temporarily see a mutation run twice.

Variations of this problem show up in the push endpoint too: we read the `lastMutationID` to know whether to process an individual mutation. But if we don't have a stable snapshot of the database inside the transaction, then we might read the value `42` for `lastMutationID` and decide to run mutation `43`, but the effects of mutation `43` are actually already in the database. We end up running the mutation twice.

## Do I really need to do this?

It is technically possible – in some cases and with great care – to implement a correct push and pull endpoint with lower isolation levels.

It requires a great deal of thought and we don't recommend it. If you still really want to do this please [contact us](https://replicache.dev/#contact).

## Why don't classic web apps have this problem?

Classic web apps often _do_ have consistency problems due to low database isolation levels. But you don't notice it as much because they are usually just requesting tiny slices of data as you move around the app.

You might see an inconsistency, but you move elsewhere in the app or refresh and it goes away.

What we're trying to do with synchronizing systems like Replicache is to **stop** sending so many requests to the server, and especially to not have to wait on them. We want to send data to the client once, and just let it just read its own copy.

In order to do that, the data the client has needs to be correct. We can't rely on reload to fix things, because reloads go to local storage, not the server!

## Rollbacks

Snapshot isolation can cause transaction rollback when two transactions try to write the same row.

In this case, all you need to do is retry the transaction. Replicache will retry pushes and pulls automatically, but in this case it's better to do it on the server. All our samples have code to do this.

See, for example, [`shouldRetryTransaction`](https://github.com/rocicorp/todo-row-versioning/blob/main/server/src/pg.ts#L135).

## Performance Considerations

The main consideration is that they can reduce write throughput if there are often transactions writing to the same value.

Our [strategies](/strategies/overview) documentation notes the write throughput of each strategy, assuming snapshot isolation.
