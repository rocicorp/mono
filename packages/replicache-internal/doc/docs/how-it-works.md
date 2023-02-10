---
title: How Replicache Works
slug: /how-it-works
---

# How Replicache Works

## The Big Picture

Replicache enables instantaneous UI and realtime updates by taking the server round-trip off the application’s critical path, and instead syncing data continuously in the background.

<p class="text--center">
  <img src="/img/diagram.png" width="650"/>
</p>

Replicache is a key-value store running in the browser. Your application reads and writes to Replicache at memory-fast speed and those changes are synchronized to the server in the background. Meanwhile, changes from other users or processes happening on the server are pulled down into Replicache, merged with local changes, and revealed to your application in a principled fashion.

The Replicache model has several parts:

1. Replicache itself: a transactional in-browser key-value store that is git-like under the hood. The git-like features enable Replicache to merge changes from the server in a principled fashion with local changes that might or might not have yet been synchronized.

2. Your application: your application’s operations are implemented using Replicache. Your app defines _mutators_, JavaScript functions encapsulating change and conflict resolution logic, and subscriptions*,* standing queries against Replicache that fire a notification when their result changes.

3. Your server: Replicache requires two server endpoints to synchronize data: a _push_ endpoint to receive mutations from users; and a _pull_ endpoint, from which users’ Replicaches retrieve changes. Servers also might implement _poke_, an optional server-to-client message that hints the client it should pull soon.

:::note

The replicache-todo starter app contains a fully functioning server you can start with, or you can implement your own by implementing push, pull, and poke.

:::

4. Sync: changes flow through the system in a loop. The user does something which causes the application to invoke one or more mutators. The mutators change local state in Replicache (potentially notifying subscriptions) and these mutations — the mutator identifier along with the parameters it was invoked with — are persisted locally. Replicache periodically pushes these mutations to the server by passing them to the server’s push endpoint, where they are applied to the server’s canonical state. Replicache periodically calls the pull endpoint and the server returns a delta from the state of the user’s Replicache to the canonical state on the server. Replicache forks its database, applies the delta from the server, and then replays on top any local mutations that have not yet been seen by the server. (Such mutations might exist if for example mutations were applied while pull was running.) The fork is then revealed as the “main” database to the app, potentially firing subscriptions, and the cycle continues.

<p class="text--center">
  <img src="/img/flow.png" width="650"/>
</p>

## Clients and Caches

An instance of the Replicache class in memory is called a client.

```
import {Replicache} from "replicache";

const rep = new Replicache({
  name: userID,
  ...
});

console.log(rep.clientID);
```

A client is identified by a unique, randomly generated `clientID`. There is typically one client (instance of Replicache) per tab. A client is ephemeral, being instantiated for the lifetime of the application in the tab. The client provides fast access to and persistence for the keys and values used by the application. Each client syncs independently, and there are no consistency guarantees across clients.

The client sits on top of an on-disk persistent cache identified by the `name` parameter to the `Replicache` constructor. Many clients can use the same underlying cache. Sharing the same cache across multiple clients de-duplicates work that the clients have to do. So for example, two tabs open to the same app with the same user would each have their own client, but should use the same underlying cache (`name`). Note that the cache is not directly visible to the application, it is an implementation detail of the client.

:::caution

It’s important to give each user of your application their own Replicache cache(s). This ensures that one user never sees or modifies data from another user.

:::

## The Client View

Each client contains an ordered map of key/value pairs called the _Client View_ that is persisted in the underlying cache. The Client View is the application data that Replicache syncs with the server. Client View keys are strings and the values are JSON-compatible values.

The size of a Client View is limited primarily by [browser policies](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API/Browser_storage_limits_and_eviction_criteria). You can store hundreds of MB in a Replicache Client View without affecting performance significantly, though HTTP request limits to your endpoints might come into play.

Access to the Client View is fast. Reads and writes generally have latency < 1ms and data can be scanned at over 500MB/s on most devices.

You do not need to keep a separate copy of the client view in memory (e.g., `useState` in React). The idea is that you read data out of Replicache and directly render it. To make changes, you modify Replicache using mutators (see below). Invocations of mutators fire subscriptions (see below) that cause the UI to re-read the relevant data and re-render the UI.

## Subscriptions

UI is typically built using the `subscribe()` method (or `useSubscribe()` in React):

```
const todos = useSubscribe(rep, async tx => {
  return await tx.scan({prefix: "todo/"}).toArray();
});
return <ul>
  { todos.map(todo => <li key={todo.id}>{todo.text}</li>) }
</ul>;
```

The subscribe method gets passed a function that receives a `ReadTransaction` parameter. You can do any number of reads from Replicache inside this function, and compute some result.

Whenever the data in Replicache changes such that a subscription is potentially out of date — either because of a local/optimistic change or because of syncing with the server — the subscription function re-runs. If the result changes, the subscription \*\*fires and the UI re-renders.

By using subscriptions to build your UI, you guarantee that the entire UI always correctly reflects the latest state, no matter why or how it changed.

## Mutations

Mutations are the way that data changes in Replicache, and are at the core of how Replicache sync works.

At startup, register one or more _mutators_ with Replicache. A mutator is a named JavaScript function that operates on Replicache. Both `createTodo` and `markTodoComplete` below are mutators.

```
const rep = new Replicache({
  ...
  mutators: {
    createTodo,
    markTodoComplete,
  },
});

async function createTodo(tx: WriteTransaction, todo: Todo) {
  await tx.put(`/todo/${todo.id}`, todo);
}

async function markTodoComplete(tx: WriteTransaction,
    {id, complete}: {id: string, complete: boolean}) {
  const key = `/todo/${id}`;
  const todo = await tx.get(key);
  if (!todo) {
    return;
  }
  todo.complete = complete;
  await tx.put(key, todo);
}
```

To change the Client View, call a mutator and pass it arguments:

```
await rep.mutate.createTodo({id: nanoid(), text: "take out the trash"});
...
await rep.mutate.markTodoComplete({id: nanoid(), complete: true});
```

This applies the changes to the Client View, causing any relevant subscriptions to re-run and fire if necessary. In React, this will cause the dependent components to re-render automatically.

Internally, calling a mutator also creates a _mutation_: a record of a mutator being called with specific arguments. For example, after the above code runs, Replicache will internally be tracking two mutations:

```
[
  {id: 1, name: "createTodo", args: {id: "t1", text: "take out the trash"}},
  {id: 2, name: "markTodoComplete", args: {id: "t1", complete: true}},
]
```

Until the mutations above are pushed by Replicache to the server during sync they are pending.

## Sync

The above sections describe how Replicache works on the client-side.

This is all you need to know to get started using Replicache using the starter app. That’s because the starter app includes a generic server that fully implements the sync protocol.

However, to use Replicache well, it’s still important to understand how sync works conceptually. And you _need_ to know this if you plan to use Replicache with your own backend, whether existing or new.

At a high level sync works as follows. Recall that a mutation is the name of a mutator that was invoked along with the arguments it was invoked with.

- When a mutation is invoked on the client, its effects are applied to the local Client View and it is assigned a sequential per-client _mutation id._ The mutation including its id is recorded as pending in the client.
- The client periodically pushes pending mutations to the server. The server has an implementation for each named mutator, and applies the mutations pushed to it from a client transactionally in the order they are received. It also records the _lastMutationID_, the high water mark of mutation ids seen from that client.
- The client periodically pulls the most recent state from the server. The server returns a delta and enough information for the client to bring its Client View up to date with the server, as well as the lastMutationId from that client, so the client knows which pending mutations to re-apply (their effects will not yet have been seen by the server).

### Push

Mutations are sent in batches to the _push endpoint_ on your server (conventionally called `replicache-push`).

The push endpoint processes the sent mutations in order by running code on the server that implements each named mutator.

:::note

The [replicache-todo](/samples/todo) starter app contains a generic push endpoint that automatically implements mutators by reusing the JavaScript mutator code from the the client. This Shared Mutator Pattern (TODO link) is very common in Replicache apps and is convenient because it means that frontend devs can add most functionality to apps without touching the server at all.

:::

If the server is not JavaScript-based, or otherwise unable to use the Shared Mutator Pattern, then the push endpoint must contain an implementation of each mutator clients might send. See BYOB (TODO) for more information.

#### Speculative Execution and Confirmation

It is important to understand that the push endpoint is _not_ expected to necessarily compute the same result that the mutator on the client did. This is a feature. The server may have newer or different state than the client has. That’s fine —- the pending mutations applied on the client are _speculative_ until applied on the server. In Replicache, the server is authoritative. The client-side mutators create speculative results, then the mutations are pushed and executed by the server creating _confirmed_, canonical results. The confirmed results are later pulled by the client, with the server-calculated state taking precedence over the speculative result.

The mechanism by which this happens is described below in Rebase.

### Pull

Periodically, Replicache requests an update to the Client View by calling the _pull endpoint_ (conventionally, `replicache-pull`).

The pull request contains a _cookie_ and the response contains a new _cookie,_ a _patch,_ and the requesting client’s _lastMutationID_.

The cookie is a value opaque to the client identifying the state from the server that the client has. It is used during pull by the server to compute a patch that brings the client’s state up to date with the server’s. Typically the cookie encapsulates the entire state of all data in the client view, not just the data for the requesting user. You can think of this as something like the “version” of the data in the backend datastore.

The lastMutationID returned in the response tells the client which of its mutations have been confirmed by the server and therefore whose results if any are represented in the patch. Upon rebasing on top of the new state (see below), the client can discard any pending mutations with mutation id ≤ lastMutationID in the pull response: they are no longer pending, they are confirmed.

#### Rebase

Once the client receives a pull response, it needs to apply the patch to the local state.

But it can’t apply the patch to the _current_ local state because that state likely includes changes caused by optimistic mutations. The patch would effectively double-apply those changes, if it can be sensibly applied at all.

To address this problem, Replicache is modeled under the hood like git. It maintains historical versions of the Client View and, like git branches, has the ability to work with a historical version of the Client View behind the scenes, and then reveal it to the application. To apply the patch from a pull response, Replicache _forks_ into a branch the canonical Client View received from the server in the previous pull. It then applies the patch received in the current pull response to arrive at the new canonical state of the Client View that the server has. (Recall that the server returns an opaque-to-the-client cookie with each pull response. This cookie is sent along with the next pull request, and is the mechanism by which the server knows how to generate the patch.)

The client now has in a branch a Client View identical to the server’s. However there might be pending mutations that have been applied locally to the main/current Client View that are as yet unconfirmed by the server (specifically, those with mutation IDs ≥ the lastMutationID in the current pull response). These pending mutations are not represented in the new Client View in the branch, but need to be, otherwise their changes will appear to be lost to the application when the new Client View is revealed. The client now replays (_rebases_ in git terms) pending mutations on top of the branched Client View, in order, to arrive at a Client View that has as a base a very recent snapshot of the server’s state, plus any pending mutations rebased on top.

:::note

It’s possible and common for mutations to calculate a different effect when they run during rebase. For example, a calendar invite may run during rebase and find that the booked room is no longer available. In this case, it may add an error message to the client view that the UI displays, or just book some different but similar room.

:::

Once all the pending mutations have been replayed, Replicache sets the fork to be the main branch the UI reads and writes to. Thus revealed, any affected subscriptions fire, and the UI updates.

The rebase step explains the mechanism by which the effect of a speculative mutation on the local Client View is superseded by the mutation’s effect when confirmed by the server. The server applies the mutation to the canonical state on the server, perhaps with different effects than the when the mutation was applied locally on the client. The client then pulls the updated, canonical server state down. Because the lastMutationID returned by pull is ≥ the speculative mutation’s id, the (now no longer) speculative mutation is discarded. It is not rebased on top of the new state which includes its canonical effects.

### Poke (optional)

Replicache can call pull on a timer (see `pullInterval`) but this is really only used in development. It’s much more common for the server to **tell** potentially-affected clients when a good time to pull is.

This is done by sending the client a hint that it should pull soon. This hint message is called a _poke_. The poke doesn’t contain any actual data. All the poke does is tell the client that it should pull again soon.

There are many ways to send pokes. The [replicache-todo](/samples/todo) starter app does it using Supabase’s built-in realtime features. However, you can also use a push service like PusherJS or a hand-rolled websocket.
