---
title: Push Endpoint Reference
slug: /reference/server-push
---

The Push Endpoint applies batches of mutations to the server.

For more information, see [How Replicache Works — Push](/concepts/how-it-works#push).

## Configuration

Specify the URL with the [`pushURL`](/api/interfaces/ReplicacheOptions#pushurl)
constructor option:

```js
const rep = new Replicache({
  // ...
  pushURL: '/replicache-push',
});
```

## Method

Replicache always fetches the push endpoint using HTTP POST:

```http
POST /replicache-push HTTP/2
```

## Request Headers

Replicache sends the following HTTP request headers with push requests:

```http
Content-type: application/json
Authorization: <auth>
X-Replicache-RequestID: <request-id>
```

### `Content-type`

Always `application/json`.

### `Authorization`

This is a string that should be used to authorize the user. It is prudent to also verify that the `clientID` passed in the `PushRequest` in fact belongs to that user. If not, and users' `clientID`s are somehow visible, a user could push mutations on behalf of another user.

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

When pushing we `POST` an HTTP request with a [JSON encoded body](/api/type-aliases/PushRequest).

```ts
type PushRequest = {
  pushVersion: 1;
  clientGroupID: string;
  mutations: Mutation[];
  profileID: string;
  schemaVersion: string;
};

type Mutation = {
  clientID: string;
  id: number;
  name: string;
  args: ReadonlyJSONValue;
  timestamp: number;
};
```

### `pushVersion`

Version of the type Replicache uses for the request body. The current version is `1`.

### `clientGroupID`

The [`clientGroupID`](/api/classes/Replicache#clientgroupid) of the requesting Replicache
client group.

### `mutations`

An array of mutations to be applied to the server, each having:

- `clientID`: The ID of the client within the group that created the mutation.
- `id`: A sequential per-client unsigned integer. Each mutation will have an ID exactly one greater than the previous one in the list.
- `name`: The name of the mutator that was invoked (e.g., from [Replicache.mutate](/api/classes/Replicache#mutate)).
- `args`: The arguments that were passed to the mutator.
- `timestamp`: The [`DOMHighResTimeStamp`](https://developer.mozilla.org/en-US/docs/Web/API/DOMHighResTimeStamp) from the source client when the mutation was initially run. This field is not currently used by the protocol.

### `profileID`

The [`profileID`](/api/classes/Replicache#profileid) of the requesting Replicache instance. All clients within a browser profile share the same `profileID`.

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
- `401` for auth error — Replicache will reauthenticate using
  [`getAuth`](/api/classes/Replicache#getauth) if available
- All other status codes are considered to be errors

Replicache will exponentially back off sending pushes in the case of both
network level and HTTP level errors.

### HTTP Response Body

The response body to the push endpoint is ignored.

## Semantics

### Unknown Client IDs

The first time a client pushes or pulls, it will have no client record on the server.

These client records could be created in either the push or pull handlers (or both), but we recommend the push handler for a few reasons:

- The pull handler can be read-only which enables useful optimizations and safety measures in many databases.
- The push handler is called less frequently so it makes sense to put the write lock there.
- Having all the writes in the push handler makes reasoning about the system easier.

See [Remote Mutations](../byob/remote-mutations) for an example implementation.

### Mutation Status

The server marks indicates that mutation was applied by returning a
[`lastMutationID`](./server-pull#lastmutationidchanges) in the `PullResponse` greater than
or equal to its mutation id.

Replicache will continue retrying a mutation until the server marks the mutation
processed in this way.

### Mutations are Atomic and Ordered

The effects of a mutation (its changes to the underlying datastore) and the corresponding update to the `lastMutationID` must be revealed atomically by the datastore. For example, in a SQL database both changes should be committed as part of the same transaction. If a mutation's effects are not revealed atomically with the update to the client's `lastMutationID`, then the sync protocol will have undefined and likely mysterious behavior.

Said another way, if the `PullResponse` indicates that mutation `42` has been processed, then the effects of mutation `42` (and all prior mutations from this client) must be present in the `PullResponse`. Additionally the effects of mutation `43` (or any higher mutation from this client) must _not_ be present in the `PullResponse`.

### Applying Mutations in Batches

The simplest way to process mutations is to run and commit each mutation and its `lastMutationID` update in its own transaction. However, for efficiency, you can apply a batch of mutations together and then update the database with their effects and the new `lastMutationID` in a single transaction. The [todo-nextjs](https://github.com/rocicorp/todo-nextjs) sample contains an example of this pattern in [push.ts](https://github.com/rocicorp/todo-nextjs/blob/main/pages/api/replicache/push.ts#L67).

### Error Handling

**If a mutation is invalid or cannot be handled, the server must still mark the
mutation as processed** by updating the `lastMutationID`. Otherwise, the client
will keep trying to send the mutation and be blocked forever.

If the server knows that the mutation cannot be handled _now_, but will be able
to be handled later (e.g., because some server-side resource is unavailable),
the push endpoint can abort processing without updating the `lastMutationID`. Replicache will consider the server offline and try again later.

For debugging/monitoring/understandability purposes, the server can _optionally_ return an appropriate HTTP error code instead of 200 e.g., HTTP 500 for internal error). However, this is for developer convenience only and has no effect on the sync protocol.

:::caution

Temporary errors block synchronization and thus should be used carefully. A
server should only do this when it definitely will be able to process the
mutation later.

:::

## Push Launch Checklist

- Ensure that the `lastMutationID` for a client is updated transactionally along
  with the pushed mutations' effects.
- All mutations with `id`s less than or equal to the client's current `lastMutationID` must
  be ignored.
- All mutations with `id`s greater than the client's current `lastMutationID+1`
  must be ignored.
- Think carefully about your error handling policy. It is possible to deadlock a
  client if it pushes a mutation that _always_ causes an error that stops
  processing. No other mutations from that client can make progress in this
  case. A reasonable default starting point might be along these lines:
  - If a temporary error is encountered that might be resolved on retry, halt
    processing mutations and return.
  - If a permanent error is encountered such that the mutation will never be
    appliable, ignore that mutation and increment the `lastMutationID` as if it
    were applied.
- Ignore all `PushRequest`s with an unexpected `pushVersion`.
