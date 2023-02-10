---
title: Pull Endpoint Reference
slug: /server-pull
---

The Pull Endpoint serves the Client View for a particular Replicache client.

For more information, see [How Replicache Works — Pull](how-it-works#%E2%91%A0-pull).

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

This is a string that can be used to authorize a user. The auth token is set
by defining [`auth`](api/interfaces/ReplicacheOptions#auth).

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
response](#http-response). This can be useful in cases where a server receives a
pull request from a client it doesn't know about (perhaps because the client
state has been deleted). In that case one thing one might do is to re-establish
the record of the client on the server side with the `lastMutationID` it is
expecting, which is this value.

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

```ts
type PullResponse = {
  cookie: JSONValue;
  lastMutationID: number;
  patch: PatchOperation[];
};
```

### `cookie`

The `cookie` is something that the backend completely controls. It gets sent to
the client in the pull response and gets sent back to the server in the next
pull request. Its main usecase is to allow the backend to efficiently compute
the diff between pull requests.

The cookie can be any [`JSONValue`](api#JSONValue) but just like with HTTP cookies
you want to limit its size since it get sent on every request.

For more information on different strategies on how to use the cookie see [Computing Changes for Pull](#TODO).

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

## Pull Launch Checklist

- Check the [Launch Checklist](launch-checklist#all-endpoints) for the checklist
  that is common for both push and pull.
- Ensure that the [`lastMutationID`](#lastmutationid-1) returned in the response
  is read in the same transaction as the client view data (ie, is consistent
  with it)
- If there is a problem with the `cookie` (e.g., it is unusable) return all
  data. This is done by first sending a [`clear`](#clear) op followed by
  multiple [`put`](#put) ops.
- Make sure that the client view is not a function of the client ID. When
  starting up Replicache, Replicache will fork the state of an existing client
  (client view and cookie) and create a new client (client view, client ID and
  cookie).
- Ignore all pull requests with an unexpected
  [`pullVersion`](server-pull#pullversion).
