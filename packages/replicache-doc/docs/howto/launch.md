---
title: Launch to Production
slug: /howto/launch
---

Before you launch with Replicache in your product, it's a good idea to double-check that you have correctly covered all the small details of integration. This list can help you determine if you might have missed a detail, or deferred and then forgotten about one.

## JS SDK

- If you wish to change the signature of a mutator (eg, the number or type of
  its arguments) you must choose a new name; Replicache does not handle mutator
  versioning.
- At some point you will almost certainly wish to change the schema of mutations
  included in the `PushRequest` and the client view returned in the
  `PullResponse`. The `ReplicacheOptions.schemaVersion` exists to facilitate
  this; it can be set by your app and is passed in both the `PushRequest` and
  `PullRequest`. Consider setting the `schemaVersion` from the start so that you
  don't later have to special case the "no schemaVersion" case.
- If a user's auth token can expire during a session, causing your endpoints to
  return a 401, be sure that re-auth is handled for **Push** and **Pull** via
  `getAuth`.
- Ensure your use of `clientID` is correct. A `clientID` represents a unique
  running instance of the `Replicache` class. Typically in applications, each
  tab _load_ gets a unique `clientID`. Do not use the `clientID` as a stable
  identifier for a user, machine, or browser profile. **Note:** In multiplayer
  applications, a common and correct application of the `clientID` is to
  represent a running session (e.g., a mouse cursor), because in most
  multiplayer applications the design goal is that two tabs from the same user
  should show up as two separate cursors to other users.
- The `name` property of `ReplicacheOptions` is required to differentiate
  Replicache instances for different users. This is important for the following
  reasons:
  - For efficiency and performance, a new `Replicache` instance will
    initialize its state from the persisted state of an existing `Replicache`
    instance with the same `name`, domain and browser profile.
  - Mutations from one `Replicache` instance may be pushed using the
    `ReplicacheOptions.auth`, `ReplicacheOptions.pushURL`,
    `ReplicacheOptions.pullURL`, `ReplicacheOptions.pusher`, and
    `ReplicacheOptions.puller` of another Replicache instance with the same
    `name`, domain and browser profile.

## All endpoints

- Ensure that you are authenticating the auth tokens configured via `ReplicacheOptions`, which are passed in the **Authentication** HTTP header.
- Your endpoints should return HTTP 401 to indicate that the user's authentication token is invalid (e.g., non-existent or expired), and that the app should re-authenticate them.
- **Ensure that the `clientID` passed in does in fact belong to the authenticated user.** Client IDs are random and cryptographically strong, but it is best to be safe. Note that the Replicache sample apps do not have a notion of a _user_, so they are missing the prudent step of associating the `clientID` with a user on the server and ensuring that the authenticated user is associated with the `clientID` that is passed in to the `Push` endpoint. If the `clientID` from one user is visible to others then you _must_ do this authentication, else one user could push mutations on behalf of another user.
- It is extremely important to ensure that your datastore and/or the way you use it guarantees the consistency and isolation properties required for Replicache to work as designed. These properties are:

  - the effects of a transaction are revealed atomically
  - within a transaction, reads are consistent, ie, reading the same item twice always results in the same value, unless changed within the transaction
  - a transaction sees the effects of all previously committed transactions

  For example, MySQL's **SERIALIZABLE** isolation level provides these guarantees.

  If would like some advice on how to set up your particular datastore correctly, or if you have any questions, please [contact us](https://replicache.dev/#contact).

- The keys in Replicache are conceptually strings encoded using UTF-8. The
  ordering of the keys when doing `scan` is a bytewise compare of UTF-8 encoded
  strings. If you are implement `ReadTransaction` (or `WriteTransaction`) in
  your own backend make sure you are treating these strings as UTF-8. We provide
  and npm package called
  [compare-utf8](https://www.npmjs.com/package/compare-utf8) which can be used
  to compare JS strings using UTF-8 bytewise comparison.

## Push endpoint

See [Push Launch Checklist](/reference/server-push#push-launch-checklist).

## Pull endpoint

See [Pull Launch Checklist](/reference/server-pull#pull-launch-checklist).
