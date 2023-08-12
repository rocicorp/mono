---
title: Per-Space Version Strategy
slug: /strategies/per-space-version
---

# 🛸 Per-Space Version Strategy

The Per-Space Version Strategy is the same as the [The Global Version Strategy](/strategies/global-version) except it has more than one space.

This increases throughput of the server. Instead of approximately 50 pushes per second across your entire server, you can get 50 pushes per space.

A common example of how people partition by space is along organizational boundaries in a SaaS application. Each customer org would be its own space and you'd thereby get 50 pushes per second per organization.

The tradeoffs to keep in mind is that you lose consistency guarantees across spaces. Replicache mutations are atomic: you can move data within a space, rename, copy, etc., and you have a guarantee that the entire change happens or none of it does. But this guarantee does not apply across spaces.

:::tip Example

Imagine moving data from one space to another. Because there is no transactional guarantees across spaces, during the move, the user might see the data exist in both spaces, or neither.

While this might just seem like a minor UI annoyance, keep in mind that it means that if you have IDs that refer to data across spaces, there is no guarantee that the data actually exists at the moment you render. You'll have to defensively guard against invalid pointers into other spaces.

:::

This is why partitioning makes most sense at very high-level boundaries, like organizations, so that it will be uncommon in your application to want to have data from two spaces interact.

## Schema

The schema generalizes the schema from the [Global Version Strategy](./reset.md):

- The `ReplicacheSpace` entity gains an `id` field.
- The `ReplicacheClientGroup` entity and each domain entity add a `spaceID` field.

```ts
type ReplicacheSpace = {
  id: string;

  // Same as Global Version Strategy.
  version: number;
};

type ReplicacheClientGroup = {
  // Same as Global Version Strategy.
  id: string;
  userID: any;

  spaceID: string;
};

type ReplicacheClient = {
  // Same as Global Version Strategy.
  id: string;
  clientGroupID: string;
  lastMutationID: number;
  lastModifiedVersion: number;
};

// Each of your domain entities will have one additional fields.
type Todo = {
  // ... fields needed for your application (id, title, complete, etc)

  // Same as Global Version Strategy.
  lastModifiedVersion: number;
  deleted: boolean;

  spaceID: string;
};
```

## Push

The push handler should receive the `spaceID` being operated on as an HTTP parameter. The logic is almost identical to the Global Version Strategy, with minor changes **marked below with bold type**.

1. Create a new `ReplicacheClientGroup` if necessary.
1. Verify that the requesting user owns the specified `ReplicacheClientGroup`.
1. **Verify that the `ReplicacheClientGroup` is in the requested space.**

Then, for each mutation described in the [`PushRequest`](/reference/server-push#http-request-body):

<ol>
  <li value="3">Create the <code>ReplicacheClient</code> if necessary.</li>
  <li>Validate that the <code>ReplicacheClient</code> is part of the requested <code>ReplicacheClientGroup</code>.</li>
  <li>Validate that the received mutation ID is the next expected mutation ID from this client.</li>
  <li><b>Increment the per-space version.</b></li>
  <li>Run the applicable business logic to apply the mutation.
    <ul>
      <li><b>For each domain entity that is changed or deleted, update its <code>lastModifiedVersion</code> to the current per-space version.</b></li>
      <li>For each domain entity that is deleted, set its <code>deleted</code> field to true.</li>
    </ul>
  </li>
  <li>Update the <code>lastMutationID</code> of the client to store that the mutation was processed.</li>
  <li><b>Update the <code>lastModifiedVersion</code> of the client to the current per-space version.</b></li>
</ol>

### Pull

The pull handler should also receive the `spaceID` being operated on as an HTTP parameter. The logic changed from the Global Version Strategy is **marked in bold type**.

<ol>
  <li>Verify that requesting user owns the requested <code>ReplicacheClientGroup</code>.</li>
  <li><b>Verify that the requested <code>ReplicacheClientGroup</code> is within the requested space.</b></li>
  <li>Return a <code><a href="/reference/server-pull#http-response-body">PullResponse</a></code> with:
    <ul>
      <li><b>The current per-space version as the cookie.</b></li>
      <li>The <code>lastMutatationID</code> for each client that has changed since the requesting cookie.</li>
      <li>A patch with:
        <ul>
          <li><code>put</code> ops for every entity created or changed since the request cookie.</li>
          <li><code>del</code> ops for every entity deleted since the request cookie.</li>
        </ul>
      </li>
    </ul>
  </li>
</ol>

## Example

[Todo-WC](https://github.com/rocicorp/todo-wc) is a simple example of per-space versioning. [Repliear](/examples/repliear) is a more involved example.

## Challenges

- Like the Global Version strategy, soft deletes can be annoying.
- Also like the Global Version strategy, it is difficult to implement features like read authentication and partial sync.
- It can be hard in some applications to find a way to partition spaces naturally.
- 50 pushes per second per space can still be insufficient for some applications.

## Variations

The same variations available to [The Global Version Strategy](/strategies/global-version#variations) apply here.
