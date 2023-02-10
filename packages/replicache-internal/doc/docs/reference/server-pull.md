---
title: Pull Endpoint Reference
slug: /reference/server-pull
---

The Pull Endpoint serves the Client View for a particular Replicache client.

For more information, see [How Replicache Works — Pull](/concepts/how-it-works#pull).

## Configuration

Specify the URL with the [`pullURL`](api/interfaces/ReplicacheOptions#pullURL) constructor option:

```js
const rep = new Replicache({
  // ...
  pullURL: '/replicache-pull',
});
```

## Method

Replicache always fetches the pull endpoint using HTTP POST:

```http
POST /replicache-pull HTTP/2
```

## Request Headers

Replicache sends the following HTTP request headers with pull requests:

```http
Content-type: application/json
Authorization: <auth>
X-Replicache-RequestID: <request-id>
```

### `Content-type`

Always `application/json`.

### `Authorization`

This is a string that should be used to authorize a user. It is prudent to also verify that the `clientID` passed in the `PushRequest` in fact belongs to that user. If not, and users' `clientID`s are somehow visible, a user could pull another user's Client View.

The auth token is set by defining [`auth`](api/interfaces/ReplicacheOptions#auth).

### `X-Replicache-RequestID`

The request ID is useful for debugging. It is of the form
`<clientid>-<sessionid>-<request count>`. The request count enables one to find
the request following or preceeding a given request. The sessionid scopes the
request count, ensuring the request id is probabilistically unique across
restarts (which is good enough).

This header is useful when looking at logs to get a sense of how a client got to
its current state.

## HTTP Request Body

When pulling we `POST` an HTTP request with a [JSON encoded body](/api#pullrequest).

```ts
type PullRequest = {
  clientID: string;
  cookie: JSONValue;
  lastMutationID: number;
  profileID: string;
  pullVersion: number;
  schemaVersion: string;
};
```

### `clientID`

The [`clientID`](api/classes/Replicache#clientID) of the requesting Replicache instance.

### `cookie`

The cookie that was received last time a pull was done. `null` if this is the first pull from this client.

### `lastMutationID`

The `lastMutationID` the client received in the last [pull
response](#http-response).

### `profileID`

The [`profileID`](api/classes/Replicache#profileid) of the requesting Replicache instance. All clients within a browser profile share the same `profileID`. It can be used for windowing the Client View, which one typically wants to do per-browser-profile, not per-client.

### `pullVersion`

Version of the type Replicache uses for the response JSON. The current version is `0`.

### `schemaVersion`

This is something that you control and should identify the schema of your client
view. This ensures that you are sending data of the correct type so that the
client can correctly handle the data.

The [`schemaVersion`](api/interfaces/ReplicacheOptions#schemaVersion) can be set
in the [`ReplicacheOptions`](api/interfaces/ReplicacheOptions) when creating
your instance of [`Replicache`](api/classes/Replicache).

## HTTP Response

### HTTP Response Status

- `200` for success
- `401` for auth error — Replicache will reauthenticate using [`getAuth`](api/classes/Replicache#getAuth) if available
- All other status codes considered errors

Replicache will exponentially back off sending pushes in the case of both network level and HTTP level errors.

### HTTP Response Body

The response body is a JSON object of the [`PullResponse`](api#PullResponse) type:

<!-- TODO(DD31): In DD31 we use ClientStateNotFoundResponse for when the client group
 cannot be found. That should only happen when you are developing the server or the
 server is using an ephemeral storage -->

```ts
export type PullResponse =
  | PullResponseOK
  | DeprecatedClientStateNotFoundResponse;

export type PullResponseOK = {
  cookie?: ReadonlyJSONValue;
  lastMutationID: number;
  patch: PatchOperation[];
};

// This response is no longer recommended. See "Handling Unknown Clients" below.
// If returned, it causes the [onClientStateNotFound](https://doc.replicache.dev/api/classes/Replicache#onclientstatenotfound) method on the client to be called.
export type DeprecatedClientStateNotFoundResponse = {
  error: 'ClientStateNotFound';
};
```

### `cookie`

The `cookie` is an opaque-to-the-client value set by the server that is returned by the client in the next `PullRequest`. The server uses it to create the patch that will bring the client's Client View up to date with the server's.

The cookie can be any [`JSONValue`](api#JSONValue) but just like with HTTP cookies
you want to limit its size since it get sent on every request.

For more information on how to use the cookie see [Computing Changes for Pull](#computing-changes-for-pull).

### `lastMutationID`

The ID of the last mutation that was successfully applied to the server from this client.

### `patch`

The patch the client should apply to bring its state up to date with the server.

Basically this should be the delta between the last pull (as identified by the request cookie) and now.

The [`patch`](api#PatchOperation) supports 3 operations:

```ts
type PatchOperation =
  | {
      op: 'put';
      key: string;
      value: JSONValue;
    }
  | {op: 'del'; key: string}
  | {op: 'clear'};
```

#### `put`

Puts a key value into the data store. The `key` is a `string` and the `value` is
any [`JSONValue`](api#JSONValue).

#### `del`

Removes a key from the data store. The `key` is a `string`.

#### `clear`

Removes all the data from the client view. Basically replacing the client view
with an empty map.

This is useful in case the request cookie is invalid or not known to the server, or in any other case where the server cannot compute a diff. In those cases, the server can use `clear` followed by a set of `put`s that completely rebuild the Client View from scratch.

## Computing Changes for Pull

A `cookie` is returned in the `PullResponse` and passed in the next pull as part of the `PullRequest`. As mentioned, the server uses the cookie to identify the state that client currently has so it can compute a diff (patch) between that state and the current state of the server. The patch is returned in the `PullResponse`.

In the simplest case, the server could not bother with `cookie`s or sending a patch, and instead just return the full Client View to the client on each pull. This is fine for tiny amounts of data or in development, but with Client Views of any significant size, this is massively inefficient and can noticably slow sync. Typically only small amounts of data in the Client View are changing at any time, so it's usually the case that the server implements a `cookie` / patching strategy so that it only returns what's changed since the last pull.

### Global Version Number Strategy

The easiest way to generate patches uses a monotonically increasing global version number to track when entities in your datastore change. On push, in a single serialized transaction, the next version number is acquired and updates to datastore entities are marked with this version. For example, you might have a `Version` column on a database table and set it to the current version when a row is inserted or modified. The pull handler returns the current version number in the `cookie` in pull. To compute the patch from a client's state to the current state of the server, select all the entities in the datastore with a version number greater than that passed in the client's `PullRequest`. This strategy requires using soft deletes.

:::caution

It is important with this strategy to ensure that transactions are in fact serialized by the global version number. For example it would _not_ be correct to use a [Postgres sequence](https://www.postgresql.org/docs/current/sql-createsequence.html) to generate this version because the sequence number changes independently from push transactions. It would allow the following anomaly:

1. Transaction A obtains sequence number 1
2. Transaction B obtains sequence number 2
3. Transaction B commits
4. Client Q pulls with cookie 0
5. Transaction A commits

Client Q receives Transaction B's changes, but then is at version 2 and thus never sees transaction A's changes. It is now permanently out of sync. For the same reason, you should not use a timestamp like SQL `NOW()` or `Date.now()` as a global version number.

:::

At scale, contention for the global version number could be a performance bottleneck. It limits the sustained rate of pushes your application can process to about `1000/<average-push-transaction-duration-in-ms>/s`.

This performance can be improved by partitioning the datastore into separate _spaces_ which each have their own global version number and which are each synced independently. For example, a project management app might partition by project.

The [example Todo app](https://github.com/rocicorp/replicache-todo/) uses this strategy, see [`createSpace()`](https://github.com/rocicorp/replicache-todo/blob/main/pages/index.tsx#L19) and the corresponding implementation in [replicache-nextjs](https://github.com/rocicorp/replicache-nextjs/blob/main/src/backend/data.ts#L77).

This simple strategy is the one we recommend starting with, and what you get by default if you start your project with the example Todo app as a base.

### Additional options

Tere are other strategies you could use to compute the patch, and we plan to document the space of possibilities better in the future. Until then, please [contact us](https://replicache.dev/#contact) if you'd like to discuss options and tradeoffs.

### Handling Unknown Clients

Replicache does not currently support [deleting client records from the server](https://github.com/rocicorp/replicache/issues/1033).

As such there is only one valid way a requesting clientID could be unknown to the server: the client is new and the record hasn't been created yet. For these new clients, our recommendation is:

1. Validate the requesting client is in fact new (`lastMutationID === 0`). If the client isn't new, then data must have been deleted from the server which is not allowed. The server should abort and return a 500.
2. Compute a patch and cookie as normal, and return `lastMutationID: 0`. The push handler [should create the client record](./server-push#unknown-client-ids) on first push.

See [Dynamic Pull](../byob/dynamic-pull) for an example implementation.

## Pull Launch Checklist

- Check the [Launch to Production HOWTO](/howto/launch#all-endpoints) for the checklist
  that is common for both push and pull.
- Ensure that the [`lastMutationID`](#lastmutationid-1) returned in the response
  is read in the same transaction as the client view data (ie, is consistent
  with it).
- If there is a problem with the `cookie` (e.g., it is unusable) return all
  data. This is done by first sending a [`clear`](#clear) op followed by
  multiple [`put`](#put) ops.
- Make sure that the client view is not a function of the client ID. When
  starting up Replicache, Replicache will fork the state of an existing client
  (client view and cookie) and create a new client (client view, client ID and
  cookie).
- Ignore all pull requests with an unexpected
  [`pullVersion`](server-pull#pullversion).
- Do not use the `clientID` to look up what information was last sent to a
  client when computing the `PullResponse`. Since a `clientID` represents a
  unique running instance of `Replicache`, that design would result in each new
  tab pulling down a fresh snapshot. Instead, use the `cookie` feature of
  `PullResponse` to uniquely identify the data returned by pull. Replicache
  internally forks the cache when creating a new client and will reuse these
  cookie values across clients, resulting in new clients being able to startup
  from previous clients' state with minimal download at startup.
