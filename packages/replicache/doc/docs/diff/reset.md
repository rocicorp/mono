---
title: The Reset Strategy
slug: /concepts/diff/reset
---

# 🤪 The Reset Strategy

The Reset Strategy is the easiest possible diff strategy: it just sends the entire client view for every pull response. Sending the entire client view for every pull is extremely inefficient, and therefore this strategy is not recommended for anything but early development or the tinyiest production applications. It's also useful for learning Replicache.

## How it Works

### Setup

- Create storage for `ReplicacheClients` in your backend database. Each record will store a `lastMutationID`, the last mutation from that client which the backend has processed.

### Push

1. Open an exclusive (serializable) transaction.
2. Read the `lastMutationID` for the calling client. If no such client exists, create one and default its `lastMutationID` to zero.
3. Iterate through each pushed mutation. For each one, validate it has the expected next mutation ID, then process the mutation.
4. Update the `lastMutationID` of the requesting client to the last processed mutation ID.

:::caution

It is important that the push happen in a serialized transaction, and that the `lastMutationID` is updated atomically as part of this transaction. If this requirement is violated, clients will end up in any incorrect state.

:::

### On Pull

1. Open an exclusive (serializable) transaction.
2. Read the entire client view for the requesting user.
3. Create a _reset patch_ - a patch with a `clear` op followed by `put` ops for each entity.
4. Return the requesting client's `lastMutationID`, the `patch`, and a `null` cookie (because the cookie isn't used by this strategy).

## Challenges

### Performance

Needless to say this isn't a practical strategy for almost any real application. It's presented here mainly for educational reasons.

## Examples

The Get Started Guide [starts out with the Reset Strategy](/byob/client-view#serving-the-client-view) (using static data) before implementing dynamic pull.
