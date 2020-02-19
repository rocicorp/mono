# Table of Contents

- [Spinner-Free Applications](#spinner-free-applications)
- [Introducing Replicache](#introducing-replicache)
- [System Overview](#system-overview)
  * [Data Model](#data-model)
  * [The Big Picture](#the-big-picture)
- [Detailed Design](#detailed-design)
  * [Replicache Client](#replicache-client)
    + [Commits](#commits)
    + [API Sketch](#api-sketch)
  * [Replicache Server](#replicache-server)
  * [Data Layer](#data-layer)
    + [Generality](#generality)
- [Data Flow](#data-flow)
  * [Sync](#sync)
  * [Mutations outside the client](#mutations-outside-the-client)
  * [Conflicts](#conflicts)
- [Constraints](#constraints)

# Spinner-Free Applications

"[Offline-First](https://www.google.com/search?q=offline+first)" describes a client/server architecture where
the application reads and writes to a local database on the device, and synchronizes with servers asynchronously whenever
there is connectivity.

These applications are highly desired by product teams and users because they are so much more responsive and
reliable than applications that are directly dependent upon servers. By using a local database as a buffer, offline-first
applications are instantaneously responsive and reliable in any network conditions.

Unfortunately, offline-first applications are also really hard to build. Bidirectional
sync is a famously difficult problem, and one which has eluded satisfying general
solutions. Existing attempts to build general solutions (Apple CloudKit, Android Sync, Google Cloud Firestore, Realm, PouchDB) all have one or more of the following serious problems:

* **Non-Convergence.** Many solutions do not guarantee that clients end up with a view of the state that is consistent with the server. It is up to developers to carefully construct a patch to send to clients that will bring them into the correct state. Client divergence is common and difficult to detect or fix.
* **Manual Conflict Resolution.** Consult the [Android Sync](http://www.androiddocs.com/training/cloudsave/conflict-res.html) or [PouchDB](https://pouchdb.com/guides/conflicts.html) docs for a taste of how difficult this is for even simple cases. Every single pair of operations in the application must be considered for conflicts, and the resulting conflict resolution code needs to be kept up to date as the application evolves. Developers are also responsible for ensuring the resulting merge is equivalent on all devices, otherwise the application ends up [split-brained](https://en.wikipedia.org/wiki/Split-brain_(computing)).
* **No Atomic Transactions.** Some solutions claim automatic conflict resolution, but lack atomic transactions. Without transactions, automatic merge means that any two sequences of writes might interleave. This is analogous to multithreaded programming without locks.
* **Difficult Integration with Existing Applications.** Some solutions effectively require a full committment to a non-standard or proprietary backend database or system design, which is not tractable for existing systems, and risky even for new systems.

For these reasons, existing sync solutions are often not practical options for application developers, leading them
to develop their own sync protocol at the application layer if they want an offline-first app. Given how expensive and risky this is, most applications delay offline-first until the business is very large and successful. Even then, many attempts fail.

# Introducing Replicache

Replicache dramatically reduces the difficulty of building offline-first applications. Replicache's goals are:
1. a truly offline-first programming model that is natural and easy to reason about
1. maximize compatability with existing application infrastructure and minimize the work to integrate

The key features that contribute to Replicant's leap in usability are:

* **Easy Integration**: Replicache runs alongside existing application infrastructure. Replicache provides bidirectional conflict-free sync between clients and servers; it does not take ownership of the data. This makes it very easy to adopt: you can try it for just a small piece of functionality, or a small slice of users, while leaving the rest of your application the same.
* **Standard Data Model**: The Replicache data model is a standard document database. From an API perspective, it's
very similar to Firestore, MongoDB, Couchbase, FaunaDB, and many others. You don't need to learn anything new, 
and can build arbitrarily complex data structures on this primitive that are still conflict-free.
* **Guaranteed Convegence**: The existing application infrastructure is the single source of truth and Replicache guarantees that after a client sync the client's state will exactly match that of the server. Developers do not need to manually track changes or construct diffs on either the client or the server.
* **Transactions**: Replicache provides full [ACID](https://en.wikipedia.org/wiki/ACID_(computer_science)) multikey read/write transactions. On the server side, transactions are implemented as REST or GraphQL APIs. On the client, transactions are implemented as deterministic programmatic functions, which are executed serially and isolated from each other.
* **Much Easier Conflict-Resolution**: Replicache is a [Convergent Causal Consistent](https://jepsen.io/consistency/models/causal) system: after synchronization, transactions are guaranteed to have run in the same order on all nodes, resulting in the same database state. This feature, combined with transaction atomicity,
makes conflict resolution much easier. Conflicts do still happen, but in many cases resolution is a natural side-effect of serialized atomic transactions. In the remaining cases, reasoning about conflicts is made far simpler. These claims have been reviewed by independent Distributed Systems expert Kyle Kingsbury of Jepsen. See [Jepsen Summary](jepsen-summary.md) and [Jepsen Article](jepsen-article.pdf).

# System Overview

Replicache is a transaction synchronization, scheduling, and execution layer that runs in a mobile app and alongside existing server-side infrastructure. It takes a page from [academic](https://www.microsoft.com/en-us/research/publication/unbundling-transaction-services-in-the-cloud/) [research](http://cs.yale.edu/homes/thomson/publications/calvin-sigmod12.pdf) that separates the transaction layer from the data or physical storage layer. The piece in the mobile app is the *client* and the existing server-side infrastructure is the *data layer*. The data layer could be as simple as a document database or could be a massive distributed system -- replicache doesn't care.

![Diagram](./diagram.png)

## Data Model

Replicache synchronizes updates to per-user *state* across an arbitrary number of clients. The state is a sorted map of key/value pairs. Keys are byte strings, values are JSON.

## The Big Picture

The Replicache client maintains a local cache of the user's state against which the application runs read and write transactions. Both read and write transactions run immediately against the local state and write transactions are additionally queued as *pending* application on the server. Periodically the client *syncs* to a *Replicache server*, pushing pending transactions to be applied and pulling updated state. Transactions flow upstream, state changes flow downstream.

The Replicache server is a proxy in front of the data layer that makes sync more efficient. During sync the Replicache server applies the pending transactions received from the client to the data layer and returns a delta for the client to apply to its state to bring it in line with the data layer.

A key feature that makes Replicache flexible and easy to adopt is that Replicache does not take ownership of the data. The data layer owns the data and is the source of truth. Replicache runs alongside an existing data layer and requires only minimal changes to it. Processes that Replicache knows nothing about can mutate state in the data layer and Replicache clients will converge on the data layer's canonical state and correctly apply client changes on top of it.

# Detailed Design

## Replicache Client

The Replicache Client maintains:

* The client ID, a unique identifier for this client
* The next transaction ordinal. Write transactions originating on a client are uniquely identified and ordered by an ordinal which increases sequentially. This ordinal ensures causal consistency with respect to the client and is used to determine which transactions the server has confirmed.
* A _bundle_ of JavaScript, provided by the app, containing _bundle functions_ implementing transactions which can be invoked by the app to read or write data.
* A versioned, transactional, deterministically iterable key/value store that keeps the user's state
  * Versioned meaning that we can go back to any previous version 
  * Versioned also meaning that we can _fork_ from a version, apply many transactions, then reveal this new version atomically (like git branch and merge)
  * Transactional meaning that we can read and write many keys atomically

### Commits

Each version of the user's state is represented as a _commit_ which has:
* An immutable view of the user's state
* A *Checksum* over the state

Commits come in two flavors, those from the client and those from the server:
* *Pending commits* represent a change made on the client that is not yet known to be applied on the server. Pending commits include the transaction that caused them so that they may be replayed on top of new confirmed commits from the server.
* *Confirmed commits* represent a state update received from the server. They carry a *State ID* uniquely identifying this version of the user's state.

### API Sketch

There are two primary APIs: the key-value interface available to JavaScript transactions run by the client and the client interface that the app uses.

```
// KVStore is accessible to JavaScript implementing transactions.
class KVStore {
  KVStore()
  bool has(String key)
  JSON get(String key)
  void put(String key, JSON value)
  List<Entry> scan(ScanOptions options)
}

// Replicache is the client interface, used by the app.
class Replicache extends KVStore {
  Replicache(AuthOpts authOpts)
  
  // Bundle registration
  Blob bundle();
  boolean setBundle(Blob blob);

  // Transaction invocation and result change notification.
  JSON exec(String functionName, List<JSON> args);
  Subscription subscribe(String functionName, List<JSON> args, void handler(JSON result));
}

struct Entry {
  String key
  JSON value
}

struct Subscription {
  void function(JSON result) handler;
  void cancel();
}

struct ScanOptions {
  String prefix
  ScanBound start
  Limit int
}

struct ScanBound {
  ScanID id
  uint64 index
}

struct ScanID {
  String value
  bool exclusive
}
```

## Replicache Server

The Replicache Server is a multitenant distributed service. It proxies transactions from the client to the data layer and sends state updates to the client in the form of deltas. The replicache server is an optimization that reduces upstream server round trips and downstream bandwidth; conceptually it is not required, though practically it is.

For each user the Replicache server maintains a *history* of previous states. Specifically it keeps the state, state id, and checksum. Note that this history need not be complete; missing entries only affect sync performance, not correctness. 

The Replicache server provides three interfaces:
* *NewClient*: generates a new client id for the client and inserts a last confirmed ordinal entry for it in the data layer (see below)
* *Push*: accepts a batch of pending transactions from the client and applies them to the data layer
* *Pull*: accepts a state id from the client indicating the last state it pulled from the server and pulls the latest user state from the data layer, computes the delta from what the client has, and returns it (if any)

## Data Layer

The data layer is a standard REST/GraphQL web service. In order to integrate Replicache the data layer must:
1. implement an interface for each transaction in the bundle (used in Push)
1. maintain a mapping from client id to last confirmed transaction ordinal (used by Push and Pull)
1. implement an interface to insert a new record into the last confirmed ordinal mapping (used by NewClient)
1. implement an interface to fetch a user's full state along with the client's last confirmed transaction ordinal (used by Pull)

Coupling between the replicache server and the data layer is minimized, for example by constructing data layer requests in the app, which can be execute nearly opaquely by the replicache server.

### Generality

As mentioned, the data layer could be a simple document database or a complicated distrubuted system. All replicache cares about is that it runs trasactions and returns the user's data as json. Beyond transactional semantics, replicache takes no opinion on where or how the data layer stores its bits. User data might be scattered across several systems under the hood, or assembled on the fly. 

# Data Flow

Data flows from the client up to the replicache server, into the data layer, and back down from the data layer to replicache server to the client. Transactions flow upstream while state updates flow downstream. Any of these processes can stop or stall indefinitely without affecting correctness.

The client tracks state changes in a git-like fashion. The replicache client keeps a *head* commit pointer representing the current state of the local key-value database. Transactions run serially against the state in the head commit. The head commmit can change in two ways:
1. write transactions: when the app runs a transaction that changes the database, the change goes into a pending commit on top of the current head. This new pending commit becomes the new head.
1. state updates: when a state update is pulled from the server replicache will:
   1. fork a new branch from the previous confirmed commit
   1. add a new confirmed commit with the latest state update to this branch; the branch now has state identical to the server
   1. filter all pending commits already seen by the server. That is, those with ordinals less than the last confirmed ordinal for this client
   1. for each remaining pending commit in order, re-run it on the new branch; this extends the new branch with a pending commit for each pending transaction
   1. set head to the end of the new branch

## Sync

In order to sync, the client first calls Push on the replicache server, passing all its pending transactions. The replicache server plays the pending transactions serially in order against the data layer. When the data layer executes a transaction it increments the client's latest confirmed transaction ordinal as part of the same transaction. If a transaction's ordinal is less than or equal to the client's last confirmed transaction ordinal or more than one more, the transaction is ignored. 

The client then calls Pull on the replicache server, passing the id of the latest server state it saw (this state id is found in its most recent confirmed commit). The replicache server retrieves the client's last transaction ordinal and the *entire* state for the user from the data layer. The replicache server then checks its history to see if it has a cached copy of the state the client has (as identified by state id). If so the new and old states are diff'd and if there is a delta a new state id is assigned, the delta is returned to the client, and the new state stored in the replicache server's history by state id. If the server doesn't have a copy of the client's data, the full user state is returned. In all cases, the client's last confirmed transaction ordinal is returned. 

If Pull returns a state update to the client, the client applies a state update as described above: it forks from the previous confirmed commit, applies the state update and any pending transactions with ordinals greater than the last confirmed transaction ordinal just received, and reveals the new state by setting head to the end of the new branch. The client can now forget about all transactions that have been confirmed, that is, all pending transactions with ordinals less than or equal to the last confirmed by the server.

## Mutations outside the client

There is nothing in the design that requires that changes to user data must come through Replicache. In fact we expect there is great utility in mutating the user's state outside of clients, eg in batch jobs or in response to changes in other users' clients. So long as all transactions that mutate the user's data run at a proper isolation level and leave the database in a valid state, replicache will faithfully converge all clients to the new state. 

## Conflicts

Conflicts are a reality of disconnected operation. There is no built in conflict resolution or signaling of exceptional conditions in replicache. Conflict resolution should be implemented in the transaction itself and any signaling that might need to take place (eg, 'this action failed, let the user know') should happen in the user data. For example, a transaction that reserves an hour on a user's calendar could keep a status for the reservation in the user's data. The transaction might successfully reserve the hour when running locally for the first time, setting the status to RESERVED. Later, if still pending, the transaction might be replayed on top of a state where that hour is unavailable. In this case the transaction might update the status to UNAVAILABLE. Later during Push when played against the data layer the transaction will settle on one value or the other, and the client will converge on the value in the data layer. App code can rely on subscriptions to receive notifications about changes to the status field, eg to trigger notification of the user or some other kind of followup such trying the next available slot.

# Constraints

**Data size** A primary constraint is the size of user data. In fetching all a user's data from the data layer during each pull, Replicache makes an explicit tradeoff of bandwidth for ease of implementation and integration. For this reason we are initially limiting user data to 20MB per user and recommend the replicache server be deployed as close to the data layer as possible.

A second concern with data size is that it might be infeasible to complete large state update downloads on unreliable or slow connections. We can imagine a variety of potential solutions to this problem but for simplicity's sake we are punting on the problem for now. (The size constraint above helps here as well.)

**Blobs** Any truly offline first system must have first class bidirectional support for binary assets aka blobs (eg, profile pictures). In some cases these assets should be managed transactionally along with the user's data: either you get all the data and all the blobs it references or you get none of it. In any case, there is presently no special support for blobs in replicache. Users who need blobs must manage the assets themselves or encode them to json strings in the user data. We plan to address this shortcoming in the future. 

**Duplicate transaction logic** You have to implement transactions twice, once in the client JavaScript and once in the data layer. Bummer. We can imagine potential solutions to this problem but it's not clear if the benefit would be worth the cost, or widely usable.
