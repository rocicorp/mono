---
title: Detailed Design
slug: /design
---

_Realtime Sync_ describes a client/server architecture in which
an application reads and writes to a local on-device database that is synchronized in realtime in the background to a server. Updates from many users are synchronized with each other through the server with low latency and high throughput, realizing a variety of benefits.

Adding realtime sync to your application enables:

- **Realtime Multiplayer Collaboration**. Popularized by Google Docs, Figma, and Notion, realtime collaboration makes applications more useful, not to mention more exciting and alive-feeling. However realtime collaboration is famously hard to build. Replicache provides the tools that make it much easier.

- **Lag-Free UI**. By executing all reads and writes against a local datastore, apps built with Replicache respond instantly, never pausing while waiting for a server. They even work if the app is...

- **Offline**. Replicache persists its cache to client-side storage, so apps start and render instantly. Local changes are synchronized opportunistically in the background so the app works online, offline, or anything in between.

Applications with realtime sync capabilities are highly desired by product teams and users because they update live, in realtime, when something changes, without the user having to refresh. Additionally, they are much more responsive and reliable than applications that are directly dependent upon servers. Realtime sync applications are instantaneously responsive regardless of the network conditions.

# Introducing Replicache

[Replicache](https://replicache.dev) dramatically reduces the difficulty of building applications with realtime sync. Replicache's goals are:

1. Providing a programming model that is natural and easy to reason about
1. Maximizing compatability with existing application infrastructure and patterns, minimizing the work to integrate

The key features that drive Replicache's increased usability:

- **Easy Integration**: Replicache runs alongside your existing application infrastructure. You keep your existing server-side stack and client-side frameworks. Replicache doesn't take ownership of data, and is not the source of truth. Its job is to provide bidirectional sync between your clients and your servers. This makes it easy to adopt: you can try it for just a small piece of functionality, or a small slice of users, while leaving the rest of your application the same.
- **The Client View**: To use Replicache, developers define a _Client View_, the data Replicache keeps cached on a specific device. Developers arrange to return a delta from a previous version of the Client View to the current version when requested, but developers do _not_ have to worry about any local changes the client may have applied. Replicache ensures that any local mutations are correctly ordered with respect to the canonical server state.
- **Transactional Conflict Resolution**: Conflicts are a fact of life when syncing, but they don't have to be painful. Replicache rewinds and replays your transactions during sync, sort of like `git rebase`. See [Conflicts](#conflicts) for more.
- **Correctness**: [Consistency guarantees](https://jepsen.io/consistency) make distributed systems easier to reason about and prevent confusing user-visible data anomalies. When properly integrated with your backend, Replicache provides for [Causal+ Consistency](https://jepsen.io/consistency/models/causal) across the entire system. This means that transactions are guaranteed to be applied _atomically_, in the _same order_, _across all clients_. Further, all clients will see an order of transactions that is compatible with _causal history_. Basically: all clients will end up seeing the same thing, and you're not going to have any weirdly reordered or dropped messages. We have worked with independent Distributed Systems expert Kyle Kingsbury of Jepsen to validate these properties of our design. See [Jepsen on Replicache](https://replicache.dev/jepsen.html).

# System Overview

Replicache is a cache that runs inside the browser and synchronizes with a web service. The web service typically already exists when Replicache is added and it could be as simple as a document database or could be a massive distributed system -- Replicache doesn't care. In this document, we refer to the web service as the _Data Layer_. An application uses an instance of the _Replicache Client_ to read from and write to the local cache, and the client synchronizes with the data layer in the background.

![Diagram](/img/diagram.png)

## Data Model

Replicache synchronizes updates to per-user _state_ across an arbitrary number of Replicache clients. The state is a sorted map of key/value pairs. Keys are strings, values are JSON. The canonical state fetched from the data layer is known as the _Client View_: the client's view of the user's data in the data layer.

## The Big Picture

The Replicache Client maintains a local cache of the user's state against which the application runs read and write transactions (often referred to as _mutations_). Both read and write transactions run immediately against the local state and mutations are additionally queued as _pending_ application on the server. In the background the client _syncs_, pushing pending mutations to the Data Layer, and pulling updated state from it. Mutations flow upstream in push and state changes flow downstream in pull.

A key feature that makes Replicache flexible and easy to adopt is that Replicache does not take ownership of the data on the server. The Data Layer owns the data, is the source of truth, and typically requires only a few small changes to work with Replicache. Processes that Replicache knows nothing about can mutate state in the Data Layer and Replicache Clients will converge on the Data Layer's canonical state and correctly apply client changes on top of it.

# Detailed Design

## Replicache Client

The Replicache Client maintains:

- The ClientID, a unique identifier for this client
- The LastMutationID. Write transactions originating on a client are uniquely identified and ordered by an ordinal which increases sequentially. This ordinal serves as an idempotency token for the Data Layer, and is used to determine which transactions the server has applied.
- The Cookie returned along with the Client View in the most recent pull. The cookie is returned to the data layer in the next pull to be used to compute a diff from the state the client has to that which the server has.
- A persistent, versioned, transactional, deterministically iterable key/value store that keeps the user's state
  - Persistent meaning that the state of the store persists across browser sessions
  - Versioned meaning that we can go back to any previous version and can _fork_ from a version, apply transactions to it, and atomically reveal the new version (like git branch and merge)
  - Transactional meaning that we can read and write many keys atomically

The client-side of the application using Replicache provides:

- Mutators: A _mutator_ is a named function that implements a write transaction. The application invokes mutators to do its work, and they read from and write to the local cache.

The server-side (data layer) of the application provides:

- The Push endpoint: the push endpoint accepts pending mutation invocations from the client and applies them to the canonical state on the server. The push endpoint has a server-side implementaiton of each client-side mutator.
- The Pull endpoint: the pull endpoint returns the latest state to the client, typically in the form of a patch to the data the client already has.

### Commits

Within the Replicache client, each version of the user's state is represented as a _commit_ which has an immutable view of the user's state.

Commits come in two flavors, those from the client and those from the server:

- _Local commits_ represent a change made by a mutator executing locally against the client's cache. The set of local commits that are not yet known to be applied in the Data Layer are known as _pending_ commits. Local commits include the _mutator name_ and _arguments_ that caused them, so that the mutator may be replayed later on top of new snapshot commits from the server if necessary.
- _Snapshot commits_ represent a state update pulled from the server. They carry a _cookie_, which the Data Layer can use to calculate the delta for the next pull.

### API Sketch

This API sketch is in TypeScript, for JavaScript bindings. A similar API would exist for every client environment we support.

```ts
class Replicache implements ReadTransaction {
  constructor({
    pushURL: string,
    pullURL: string,
    auth: string,
    // Registers the mutators, which are used to make changes to the data.
    mutators: {[name: string]: MutatorImpl}
  });

  // Subcribe to changes to the underlying data. Every time the underlying data changes onData is called.
  // The function is also called once the first time the subscription is added.
  subscribe<R, E>(
    body: (tx: ReadTransaction) => Promise<R>,
    onData: (result: R) => void,
  ): void;
}

// A Replicache "mutator" function is just a normal JS function that accepts any JSON value, makes changes
// to Replicache, and returns a JSON value. Users can invoke mutators themselves, via the `mutate` property
// of the `Replicache` instance. Also Replicache will itself invoke these functions during sync as part of
// conflict resolution.
type MutatorImpl<Return extends JSONValue | void, Args extends JSONValue> = (
  tx: WriteTransaction,
  args?: Args,
) => MaybePromise<Return>;

interface ReadTransaction {
  get(key: string): Promise<JSONValue | undefined>;
  has(key: string): Promise<boolean>;
  scan(startAt: string): Promise<[string, JSONValue][]>;
}

interface WriteTransaction extends ReadTransaction {
  del(key: string): Promise<void>;
  get(key: string): Promise<JSONValue | undefined>;
  has(key: string): Promise<boolean>;
  put(key: string, value: JSONValue): Promise<void>;
}
```

## Data Layer

We expect the data layer to typically be a familiar REST/GraphQL web service, but it could be anything that provides transactional storage. In order to integrate Replicache, the Data Layer must:

1. maintain a mapping from ClientID to LastMutationID (used by Push and Pull)
1. implement the Pull endpoint from which the client fetches a user's Client View and its LastMutationID
1. implement the Push endpoint which executes a batch of mutators pushed upstream by the client

### Generality

As mentioned, the Data Layer could be a simple document database or a complicated distrubuted system. All Replicache cares about is that it runs trasactions and returns the user's data as json in the Client View. Beyond transactional semantics, Replicache takes no opinion on where or how the Data Layer stores its bits. User data might be scattered across several systems under the hood, or assembled on the fly.

# Data Flow

Data flows from the client up to the Data Layer, and back down from the Data Layer to the Client. Mutations are pushed upstream while state updates are pulled downstream. Either of these processes can stop or stall indefinitely without affecting correctness.

The client tracks state changes in a git-like fashion. The Replicache Client has a _main_ branch of commits and keeps a _head_ commit pointer representing the current state of the local key-value database. Transactions run against the state in the head commit. The head commmit can change in two ways:

1. write transactions (mutations): when the app runs a mutator that changes the database, the change goes into a pending commit on top of the current head. This new pending commit becomes the new head.
1. pull: when a new state update is pulled from the server Replicache will:
   1. fork a new branch (the _sync branch_) from the most recent snapshot
   1. add a new snapshot with the new state update to the sync branch; the branch now has state identical to the server
   1. compute the set of mutations to replay on the sync branch by filtering all pending commits on main that have already been applied by the server. That is, find all pending commits on main whose MutationID is greater than the LastMutationID of the new snapshot.
   1. for each mutation to replay, in order, apply it on the sync branch; this extends the sync branch with a pending commit for each mutation not yet seen by the server
   1. make the sync branch main by setting head of main to the head of the sync branch

## Syncing

There are two parts to sync: push and pull.

To push, the client invokes the Data Layer's Push endpoint, passing all its pending mutations. The Data Layer executes the pending mutations serially. When the Data Layer executes a mutation it sets the client's LastMutationID to match the mutation's ID as part of the same transaction. If a MutationID is less than or equal to the client's LastMutationID or more than one more, the mutation is ignored.

To pull, the Data Layer's Pull endpoint is invoked by the client, passing the cookie from its most recent Snapshot (as found in the most recent Snapshot commit). The Data Layer computes and returns a delta to the Client View using the cookie, and the LastMutationID for this client. The client applies the delta from Pull as described above: it forks from the previous snapshot commit, applies any local mutations that are still pending (those with mutation ids greater than the LastMutationID indicated along with the client view patch), and reveals the new state by setting head of main to the end of the new branch. The client can now forget about all pending mutations that have been confirmed, that is, all pending mutations with MutationIDs less than or equal to the LastMutationID of the most recent snapshot.

## Mutations outside the client

There is nothing in the design that requires that changes to user data must come through Replicache. In fact we expect there is great utility in mutating the user's state outside of clients, eg in batch jobs or in response to changes in other users' clients. So long as all transactions that mutate the user's data run at a proper isolation level, leave the database in a valid state, and are correctly reflected by Pull, Replicache will faithfully converge all clients to the new state.

## Push endpoint

By design, Replicache places a minimum of constraints on the Data Layer's Push endpoint. For correctness, it must execute mutations in order and ensure that the LastMutationID is updated transactionally along with any effects. Beyond that, Replicache imposes no requirements. For example, the Push endpoint need not be synchronous; it could accept a batch of mutations, enqueue them for execution elsewhere, and return. Similarly, the Push endpoint need not be consistent with the ClientView endpoint; as long as a mutation's effects and the change to LastMutationID are revealed to the ClientView endpoint atomically, the ClientView can lag or flap without affecting correctness.

## Conflicts

Conflicts are an unavoidable part of disconnected systems, but they don't need to be exceptionally painful.

A common initial approach to conflict resolution is to attempt to merge the _effects_ of divergent forks. This doesn't work well because if all you have are the effects of two forks, it can be difficult or impossible to reason about what the correct merge is.

Imagine a simple database consisting of only a single integer set to the value `1`. A client goes offline for awhile and through a series of changes, ends up with the value `2`. Meanwhile the server value goes through a series of changes and ends at `0`:

```
... - 1 - ... 0 <- server
        \ ... 2 <- offline client
```

What is the correct resolution? We can't possibly know without more information about what the _intent_ of those changes were. Were they adding? setting? multiplying? clearing? In real life applications with complex data models, many developers, and many versions of the application live at once, this problem is much worse.

A better strategy is to capture the _intent_ of changes. Replicache embraces this idea by recording, alongside each change, the name of the function that created the change along with the arguments it was passed. For example, in our example above, Replicache might have recorded the following history of pending mutations on the client:

```js
[
  {id: 1, name: 'add', key: 'myNumber', args: 2, result: {myNumber: 3}},
  {id: 2, name: 'subtract', key: 'myNumber', args: 1, result: {myNumber: 2}},
];
```

Now we have a lot better idea of what happened here!

Each `name` refers to a mutator which is just an arbitrary JavaScript function. Developers can provide whatever mutators they like, containing arbitrarily complex logic that reads and writes to the database. Mutators can realize their intended effect differently depending on the state of the database when they run.

When these mutations get pushed to the server, they get replayed in order. So on the server, we end up with the merged result: `0 + 2 - 1 -> 1`. This approach of resolving conflicts by replaying functions is very powerful and generalizes to much more complex problems.

For a more interesting example, consider a transaction that reserves an hour on a user's calendar could keep a status for the reservation in the user's data. The transaction might successfully reserve the hour when running locally for the first time, setting the status to RESERVED. Later, if still pending, the transaction might be replayed on top of a state where that hour is unavailable. In this case the transaction might update the status to UNAVAILABLE. Later during push when played against the Data Layer the transaction will settle on one value or the other, and the client will converge on the value in the Data Layer. App code can rely on subscriptions to keep the UI correctly reflective of the reservation status, or to trigger notification of the user or some other kind of followup such trying the next available slot.

We believe the Replicache model for dealing with conflicts — to have defensively written, programmatic transaction logic that is replayed atop the latest state — leads to fewer actual conflicts in practice. Our experience is that it preserves expressiveness of the data model and is far easier to reason about than other general models for avoiding or minimizing conflicts.

# Constraints

**Data size** Although there is no limit to the amount of data that can be synced by Replicache, for some applications, syncing all data the user has access is impractical. For these use cases, we advise users to maintain per-client state encoding the extent of the data that should be synced. Initially the extent can be relatively small, but as the user moves through the app, the extent can be widened. For example, if the app is a game, the extent might initially be the first level. But as the user progresses through the game, the extent widens one level at a time. It is also possible to purge data from Replicache using the same mechanism, if managing max cache size is a concern.

A second concern with data size is that it might be infeasible to complete large state update downloads on unreliable or slow connections. We can imagine a variety of potential solutions to this problem but for simplicity's sake we are punting on the problem for now. (The size constraint above helps here as well.)

**Blobs** A realtime sync engine should have first class bidirectional support for binary assets aka blobs (eg, profile pictures). In some cases these assets should be managed transactionally along with the user's data: either you get all the data and all the blobs it references or you get none of it. In any case, there is presently no special support for blobs in Replicache. Users who need blobs are advised to base64 encode them as JSON strings in the user data. We plan to address this shortcoming in the future.

**Duplicate transaction logic** The core Replicache protocol requires implementing each mutation twice, once in the app and once in the Data Layer. This is unfortunate, but a tradeoff required for the properties Replicache provides. Developers can work around this by sharing code between client and server. In the extreme case where the server is JavaScript, it can even feel like zero server code has to get written because Replicache mutations can be reused as-is.
