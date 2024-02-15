---
title: Overview
slug: /strategies/overview
---

# Backend Strategies

Replicache defines abstract [push](/reference/server-push.md) and [pull](/reference/server-pull.md) endpoints that servers must implement to sync. There are a number of possible strategies to implement these endpoints with different tradeoffs.

The main difference between the strategies is how they calcuate the `patch` required by the pull endpoint. Different approaches require different state to be stored in the backend database, and different logic in the push and pull endpoints.

Also some use-cases are only supported well with some strategies. Notably:

- **Read Auth:** When not all data is accessible to all users. In an application like Google Docs, read authorization is required to implement the fact that a private doc must be shared with you before you can access it.

- **Partial Sync:** When a user only syncs _some_ of the data they have access to. In an application like GitHub, each user has access to many GB of data, but only a small subset of that should be synced to the client at any point in time.

Here are the strategies in increasing order of implementation difficulty:

## 🤪 Reset Strategy

- **When to use:** For apps with very small amounts of data, or where the data changes infrequently. Also useful for learning Replicache.
- **Implementation:** 👍🏼 Easy.
- **Performance:** 👎🏼 Each pull computes and retransmits the entire client view.
- **Read Auth:** 👍🏼 Easy.
- **Partial sync:** 👍🏼 Easy.

**[Get started with the Reset Strategy →](./reset)**

## 🌏 Global Version Strategy

- **When to use:** Simple apps with low concurrency, and where all data is synced to all users.
- **Performance:** 👎🏼 Limited to about 50 pushes/second across entire app.
- **Implementation:** 👍🏼 Easy.
- **Read Auth:** 👎🏼 Difficult.
- **Partial sync:** 👎🏼 Difficult.

**[Get started with the Global Version Strategy →](./global-version)**

## 🛸 Per-Space Version Strategy

- **When to use:** Apps where data can be naturally partitioned into _spaces_, where all users in a space sync that space in its entirety. For example, in an app like GitHub, each repository might be a space.
- **Performance:** 🤷‍♂️ Limited to about 50 pushes/second/space.
- **Implementation:** 👍🏼 Easy.
- **Read Auth:** 🤷‍♂️ You can restrict access to a space to certain users, but all users within a space see everything in that space.
- **Partial sync:** 🤷‍♂️ You can choose which spaces to sync to each client, but within a space all data is synced.

**[Get started with the Per-Space Version Strategy →](./per-space-version)**

## 🛸 Row Version Strategy

- **When to use:** Apps that need greater performance, fine-grained read authorization, or partial sync that can't be served by per-space versioning. This is the most flexible and powerful strategy, but also the hardest to implement.
- **Performance:** 👍🏼 Close to traditional web app.
- **Implementation:** 👎🏼 Most difficult.
- **Read Auth:** 👍🏼 Fully supported. Each individual data item can be authorized based on arbitrary code.
- **Partial sync:** 👍🏼 Fully supported. Sync any arbitrary subset of the database based on any logic you like.

**[Get started with the Row Version Strategy →](./row-version)**
