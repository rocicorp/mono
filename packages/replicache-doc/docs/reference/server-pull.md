---
title: Pull Endpoint Reference
slug: /reference/server-pull
---

The Pull Endpoint serves the Client View for a particular Replicache client.

For more information, see [How Replicache Works — Pull](/concepts/how-it-works#pull).

## Configuration

Specify the URL with the [`pullURL`](/api/interfaces/ReplicacheOptions#pullurl) constructor option:

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

The auth token is set by defining [`auth`](/api/interfaces/ReplicacheOptions#auth).

### `X-Replicache-RequestID`

The request ID is useful for debugging. It is of the form
`<clientid>-<sessionid>-<request count>`. The request count enables one to find
the request following or preceeding a given request. The sessionid scopes the
request count, ensuring the request id is probabilistically unique across
restarts (which is good enough).

This header is useful when looking at logs to get a sense of how a client got to
its current state.

## HTTP Request Body

When pulling we `POST` an HTTP request with a [JSON encoded body](/api/type-aliases/PullRequest).

```ts
type PullRequest = {
  pullVersion: 1;
  clientGroupID: string;
  cookie: JSONValue;
  profileID: string;
  schemaVersion: string;
};
```

### `pullVersion`

Version of the type Replicache uses for the response JSON. The current version is `1`.

### `clientGroupID`

The [`clientGroupID`](/api/classes/Replicache#clientgroupid) of the requesting Replicache
client group.

### `cookie`

The cookie that was received last time a pull was done. `null` if this is the first pull from this client.

### `profileID`

The [`profileID`](/api/classes/Replicache#profileid) of the requesting Replicache instance. All clients within a browser profile share the same `profileID`. It can be used for windowing the Client View, which one typically wants to do per-browser-profile, not per-client.

### `schemaVersion`

This is something that you control and should identify the schema of your client
view. This ensures that you are sending data of the correct type so that the
client can correctly handle the data.

The [`schemaVersion`](/api/interfaces/ReplicacheOptions#schemaversion) can be set
in the [`ReplicacheOptions`](/api/interfaces/ReplicacheOptions) when creating
your instance of [`Replicache`](/api/classes/Replicache).

## HTTP Response

### HTTP Response Status

- `200` for success
- `401` for auth error — Replicache will reauthenticate using [`getAuth`](/api/classes/Replicache#getauth) if available
- All other status codes considered errors

Replicache will exponentially back off sending pushes in the case of both network level and HTTP level errors.

### HTTP Response Body

The response body is a JSON object of the [`PullResponse`](/api/type-aliases/PullResponse) type:

```ts
export type PullResponse =
  | PullResponseOK
  | ClientStateNotFoundResponse
  | VersionNotSupportedResponse;

export type PullResponseOK = {
  cookie: Cookie;
  lastMutationIDChanges: Record<ClientID, number>;
  patch: PatchOperation[];
};

export type Cookie =
  | null
  | string
  | number
  | (ReadonlyJSONValue & {readonly order: number | string});

/**
 * In certain scenarios the server can signal that it does not know about the
 * client. For example, the server might have lost all of its state (this might
 * happen during the development of the server).
 */
export type ClientStateNotFoundResponse = {
  error: 'ClientStateNotFound';
};

/**
 * The server endpoint may respond with a `VersionNotSupported` error if it does
 * not know how to handle the {@link pullVersion}, {@link pushVersion} or the
 * {@link schemaVersion}.
 */
export type VersionNotSupportedResponse = {
  error: 'VersionNotSupported';
  versionType?: 'pull' | 'push' | 'schema' | undefined;
};
```

### `cookie`

The `cookie` is an opaque-to-the-client value set by the server that is returned by the client in the next `PullRequest`. The server uses it to create the patch that will bring the client's Client View up to date with the server's.

The cookie must be orderable (string or number) or an object with a special `order` field with the same constraints.

For more information on how to use the cookie see [Computing Changes for Pull](#computing-changes-for-pull).

### `lastMutationIDChanges`

A map of clients whose `lastMutationID` have changed since the last pull.

### `patch`

The patch the client should apply to bring its state up to date with the server.

Basically this should be the delta between the last pull (as identified by the request cookie) and now.

The [`patch`](/api/type-aliases/PatchOperation) supports 3 operations:

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
any [`JSONValue`](/api/type-aliases/JSONValue).

#### `del`

Removes a key from the data store. The `key` is a `string`.

#### `clear`

Removes all the data from the client view. Basically replacing the client view
with an empty map.

This is useful in case the request cookie is invalid or not known to the server, or in any other case where the server cannot compute a diff. In those cases, the server can use `clear` followed by a set of `put`s that completely rebuild the Client View from scratch.

## Computing Changes for Pull

See [Diff Strategies](/strategies/overview) for information on different approaches to implementing pull.

## Handling Unknown Clients

Replicache does not currently support [deleting client records from the server](https://github.com/rocicorp/replicache/issues/1033).

As such there is only one valid way a requesting clientID could be unknown to the server: the client is new and the record hasn't been created yet. For these new clients, our recommendation is:

1. Validate the requesting client is in fact new (`lastMutationID === 0`). If the client isn't new, then data must have been deleted from the server which is not allowed. The server should abort and return a 500.
2. Compute a patch and cookie as normal, and return `lastMutationID: 0`. The push handler [should create the client record](./server-push#unknown-client-ids) on first push.

See [Dynamic Pull](../byob/dynamic-pull) for an example implementation.

## Pull Launch Checklist

- Check the [Launch to Production HOWTO](/howto/launch#all-endpoints) for the checklist
  that is common for both push and pull.
- Ensure that the `lastMutationID` returned in the response
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
