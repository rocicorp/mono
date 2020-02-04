# Table of Contents

* [Offline-First for Every Application](#offline-first-for-every-application)
* [Introducing Replicache](#introducing-replicache)
* [System Overview](#system-overview)
  * [Replicache Client](#replicache-client)
  * [Replicache Server](#replicache-service)
  * [Customer Server](#customer-service)
* [Synchronization](#synchronization)
  * [Server Pull](#server-pull)
  * [Client Pull](#client-pull)
  * [Client Push](#client-push)
  * [Server Push](#server-push)
  * [Pokes](#pokes)
* [Conflict Resolution](#conflict-resolution)

# Offline-First for Every Application

"[Offline-First](https://www.google.com/search?q=offline+first)" describes a client/server architecture where
the application reads and writes to a local database on the device, and synchornizes with servers asynchronously whenever
there is connectivity.

These applications are highly desired by product teams and users because they are so much more responsive and
reliable than applications that are directly dependent upon servers. By using a local database as a buffer, offline-first
applications are instantaneously responsive and reliable in any network conditions.

Unfortunately, offline-first applications are also really hard to build. Bidirectional
sync is a famously difficult problem, and one which has elluded satisfying general
solutions. Existing attempts to build general solutions (Apple CloudKit, Android Sync, Google Cloud Firestore, Realm, PouchDB) all have one or more of the following serious problems:

* **Non-Convergence.**
* **Manual Conflict Resolution.** Consult the [Android Sync](http://www.androiddocs.com/training/cloudsave/conflict-res.html) or [PouchDB](https://pouchdb.com/guides/conflicts.html) docs for a taste of how difficult this is for even simple cases. Every single pair of operations in the application must be considered for conflicts, and the resulting conflict resolution code needs to be kept up to date as the application evolves. Developers are also responsible for ensuring the resulting merge is equivalent on all devices, otherwise the application ends up [split-brained](https://en.wikipedia.org/wiki/Split-brain_(computing)).
* **No Atomic Transactions.** Some solutions claim automatic conflict resolution, but lack atomic transactions. Without transactions, automatic merge means that any two sequences of writes might interleave. This is analogous to multithreaded programming without locks.
* **Difficult Integration with Existing Applications.** Some solutions effectively require a full committment to a non-standard or proprietary backend database or system design, which is not tractable for existing systems, and risky even for new systems.

For these reasons, existing products are often not practical options for application developers, leaving them
forced to develop their own sync protocol at the application layer if they want an offline-first app. Given how expensive and risky this is, most applications delay offline-first until the business is very large and successful. Even then, many attempts fail.

# Introducing Replicache

Replicache dramatically reduces the difficulty of building offline-first applications.

The key features that contribute to Replicant's leap in usability are:

* **Guaranteed Convegence**
* **Transactions**: Replicache models change in the system as full [ACID](https://en.wikipedia.org/wiki/ACID_(computer_science)) multikey read/write 
transactions. Transactions in Replicache are expressed as arbitrary functions, which are executed serially and isolated from 
each other.
* **Much Easier Conflict-Resolution**: Replicant is a [Convergent Causal Consistent](https://jepsen.io/consistency/models/causal) system: after synchronization, transactions are guaranteed to have run in the same order on all nodes, resulting in the same database state. This feature, combined with transaction atomicity,
makes conflict resolution much easier. Conflicts do still happen, but in many cases resolution is a natural side-effect of serialized atomic transactions. In the remaining cases, reasoning about conflicts is made far simpler. These claims have been reviewed by independent Distributed Systems expert Kyle Kingsbury of Jepsen. See [Jepsen Summary](jepsen-summary.md) and [Jepsen Article](jepsen-article.pdf).
* **Standard Data Model**: The Replicant data model is a standard document database. From an API perspective, it's
very similar to Google Cloud Firestore, MongoDB, Couchbase, FaunaDB, and many others. You don't need to learn anything new, 
and can build arbitrarily complex data structures on this primitive that are still conflict-free.
* **Easy Integration**: Replicant is a simple primitive that runs along side any existing stack. Its only job is to provide bidirectional conflict-free sync between clients and servers. This makes it very easy to adopt: you can try it for just a small piece of functionality, or a small slice of users, while leaving the rest of your application the same.

# System Overview

# TransactionIDs

Replicache commits are identified by a _TransactionID_. A Transaction ID has two components:

* A *Client ID*: Client IDs are generated by Replicache Server and assigned to clients on first sync.
* A *Transaction Ordinal*: An incrementing integer uniquely identifying each transaction on a client.

# Replicache Client

The Replicache Client maintains:

* The current client ID
* A versioned, transactional key/value store
  * Versioned meaning that we can go back to any previous version
  * Transactional meaning that we can make many changes atomically
  * And fork from some previous version, build on that, then make that the state

Each commit in the client can be of two types:
* *Snapshots* represent the current state of the server at some moment in time
* *Pending Transactions* represent a change made on the client-side that have not yet been sent to the server

Both transaction types contain:
* An immutable snapshot of the cache at that moment
* The *Transaction ID* of the last transaction applied to create the snapshot
* An *LtHash* over the entire key/value store

Additionally, transaction commits contain:
* The name of the JavaScript function plus arguments that was used to create this change

# Replicache Server

The Replicache Server is a multitentant distributed service. It maintains state for each connected client. For each client:

* A history of recent snapshots
* A queue of pending mutations

For each snapshot:

* The *TransactionID*
* The *LtHash*
* The snapshot of key/value pairs

For each pending mutation:

* The *TransactionID*
* The URL to invoke
* The payload to send

# Customer Server

The Customer Server is a standard REST/GraphQL web service. In order to integrate with Replicache is must maintain the following state:

* A mapping of ClientID/TransactOrdinal pairs

Additionally each mutation API that Replicache can call must be modified to look for an `X-Replicache-TransactionID` header and manage this table. For additional details, see [Server Push](#server-push).
