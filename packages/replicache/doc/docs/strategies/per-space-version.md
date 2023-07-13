---
title: Per-Space Version Strategy
slug: /concepts/strategies/per-space-version
---

# 🛸 Per-Space Version Strategy

The Per-Space Version Strategy is the same as the [The Global Version Strategy](/concepts/strategies/global-version) except it partitions the database into separate _spaces_ and gives each space its own version number.

This increases throughput of the server. Instead of approximately 50 pushes per second across your entire server, you can get 50 pushes per **space**.

A common example of how people partition by space is along organizational boundaries in a SaaS application. Each customer org would be its own space and you'd thereby get 50 pushes per second per organization.

The tradeoffs to keep in mind is that you lose consistency guarantees across spaces. Replicache mutations are atomic: you can move data within a space, rename, copy, etc., and you have a guarantee that the entire change happens or none of it does. But this guarantee does not apply across spaces.

:::tip Example

Imagine moving data from one space to another. Because there is no transactional guarantees across spaces, during the move, the user might see the data exist in both spaces, or neither.

While this might just seem like a minor UI annoyance, keep in mind that it means that if you have IDs that refer to data across spaces, there is no guarantee that the data actually exists at the moment you render. You'll have to defensively guard against invalid pointers into other spaces.

:::

This is why partitioning makes most sense at very high-level boundaries, like organizations, so that it will be uncommon in your application to want to have data from two spaces interact.

## How it Works

### Setup

1. Setup database as-per [The Global Version Strategy](/concepts/strategies/global-version), except instead of a single global version, add storage for `Space` entities. Each space will have a unique ID and also a `version`. You may be able to simply extend some existing entity in your database, such as an organization.
2. Each entity in your database that will be synced must be part of one (and only one) space. Add a `spaceID` attribute to each entity to keep track of the space it is part of.
3. When constructing Replicache, specify a `name` that includes the `spaceID`, for example: `${userID}/${spaceID}` so that if a user moves between spaces, the data from two spaces won't get mixed.
4. Also include the `spaceID` in the push and pull URLs so that the server will know which space to look in.

### On Push

- Same as [The Global Version Strategy](/concepts/strategies/global-version) except read and update the version from the relevant space, rather than the single global version.

### On Pull

- Same as [The Global Version Strategy](/concepts/strategies/global-version) except return only data from the requested space.

## Challenges

- It can be hard in some applications to find a way to partition spaces naturally.
- 50 pushes per second per space can still be insufficient for some applications.

## Variations

The same variations available to [The Global Version Strategy](/concepts/strategies/global-version#variations) apply here.

## Examples

- [React TODO](/examples/todo) and [Repliear](/examples/repliear) both use the space strategy to give each visitor to the sample their own unique data to play with.
