---
title: Consistency Model
slug: /concepts/consistency
---

The [Consistency Model](https://en.wikipedia.org/wiki/Consistency_model) of a distributed system like Replicache describes the guarantees the system makes about how operations are applied within the system.

Replicache was designed in consultation with indepedent distributed systems expert [Kyle Kingsbury](https://aphyr.com/about) of [Jepsen](https://jepsen.io/). When properly integrated with your backend, Replicache provides [Causal+ Consistency](https://jepsen.io/consistency/models/causal) — one of the strongest consistency models possible in a synchronizing system. Causal+ Consistency essentially guarantees that the system is:

- **Causal**: causally-related operations (mutations) always appear in their same causal order on all clients
- **Convergent**: clients always converge on the same ordering of operations
- **Progressive**: clients see progressively newer states of the world, and never see operations out of order

Below find Jepsen's summary.

## Jepsen on Replicache

[Jepsen](https://jepsen.io/) has evaluated Replicache's preliminary, internal design documents, but has not evaluated Replicache's actual code or behavior. As of October 25, 2019, Replicache's documentation describes a set of client libraries and an HTTP server for writing stateful, offline-first mobile applications against typical web services. Replicache uses arbitrary JavaScript transactions over a versioned document store on the client, and expects the web service to provide corresponding server-side transaction implementations.

Like [Bayou](https://people.cs.umass.edu/~mcorner/courses/691M/papers/terry.pdf) and [Eventually Serializable Data Services](https://groups.csail.mit.edu/tds/papers/Lynch/podc96-esds.pdf), Replicache works towards a totally ordered prefix of _final_ transactions, while _tentative_ transactions, which have not yet been totally ordered, go through a shifting series of [causally consistent](https://jepsen.io/consistency/models/causal) orders after the locally-known final prefix of the total order.

Replicache's state is always the product of _some_ order of atomically executed transactions, which simplifies proving some invariants. Tentative transactions execute speculatively, with causal consistency, but may be reordered, and re-executed arbitrarily many times, before their final order is known. This means their safety properties must hold under any (causally consistent) ordering of concurrent and future transactions. Tentative transactions can be thought of as an implementation of [Statebox](https://github.com/mochi/statebox), but with causally consistent transaction ordering. Likewise, any [CRDT](https://hal.inria.fr/inria-00609399v1/document) can be implemented in Replicache tentative transactions alone, making them equivalent to CRDTs. However, Replicache's eventually serializable transaction order provides the ability to _upgrade_ selected transactions to [strict serializability](https://jepsen.io/consistency/models/strict-serializable), at the cost of having to block for server acknowledgement. This could allow users to write hybrid commutative & non-commutative systems. Replicache's API does not expose an API for serializable transactions yet, but the listener API could, we suspect, make this possible.

Casual+ is one of the strongest consistency models offline clients can ensure, and Jepsen is pleased to see additional interest in the consistency challenges of distributed mobile computing.
