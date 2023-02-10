---
title: How Replicache Works
slug: /how-it-works
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<p class="text--center">
  <img src="/img/diagram.png" width="650"/>
</p>

Replicache is a persistent key/value store that lives inside your web app. Use it as your app's state, in place of things like MobX, Redux, Recoil, Apollo, etc.

You don't need REST or GraphQL APIs when you use Replicache either.

Instead, read and write directly to Replicache, as if it was in-memory data. Replicache continuously persists itself locally and synchronizes with your server using two special endpoints: `replicache-push` and `replicache-pull`.

## ① Pull

<Tabs
groupId="lang"
defaultValue="response"
values={[
{label: 'Request', value: 'request'},
{label: 'Response', value: 'response'},
]}>
<TabItem value="request">

```http
POST /replicache-client-view HTTP/2
Host: myservice.com
Authorization: Bearer 51857000befac83d338df7661d00d81011d7fb10

{
  "clientID": "d1570b3a",
  "cookie": "",
}
```

  </TabItem>
  <TabItem value="response">

```http
HTTP/2 200
Content-Type: application/json

{
  "lastMutationID": 6,
  "cookie": "eae66b62",
  "patch": [
    {
      "op": "put",
      "key": "todo-39b224c1",
      "value": {
        "text": "Take out the garbage",
        "complete": false
      }
    },
    {
      "op": "put",
      "key": "todo-77173e44",
      "value": {
        "text": "Walk the dog",
        "complete": true
      }
    },
    {
      "op": "del",
      "key": "todo-5546afd4"
    }
  }
}
```

  </TabItem>
</Tabs>

Understanding Replicache starts with the _Client View_.

The Client View is the set of key/value pairs that Replicache is synchronizing at any point in time. It is the Replicache Client's _view_ of the server's data. Typically there is a 1-1 correspondence between items in the server's datastore and items in the Client View, for example rows in the server's database and key/value pairs in the Client View.

The server is the _authoritative_ source of the Client View. With the exception of local mutations which are discussed below, the app will see the Client View returned by the server. In this sense the server is the source of truth for the Client View -- all clients will converge on the data it returns.

Replicache _pulls_ updates to the Client View periodically from the `/replicache-pull` endpoint on your server. Return a JSON cookie describing the last pulled state with each response.

When Replicache pulls the next time, it sends the cookie in the request. Use it to find and return the changes that have happened server-side since last pull.

:::note

You don't need to worry about clobbering the effects of unpushed ("pending") local mutations in the client with server-side changes. Replicache rewinds pending local mutations during sync and replays them "on top" of new server side state.

:::

## ② Subscribe

<Tabs
groupId="lang"
defaultValue="react"
values={[
{label: 'React', value: 'react'},
{label: 'Vanilla JS', value: 'vanilla'},
]}>
<TabItem value="react">

```js
const todos = useSubscribe(replicache, tx =>
  tx.scan({prefix: '/todo/'}).entries().toArray(),
);

return (
  <ul>
    {todos.map(t => (
      <li>{t.title}</li>
    ))}
  </ul>
);
```

</TabItem>
<TabItem value="vanilla">

```js
replicache.subscribe(
  async tx => await tx.scan({prefix: '/todo/'}).entries().toArray(),
  {
    onData: todos => {
      this.setState({todos});
    },
  },
);
```

  </TabItem>
</Tabs>

Build your UI using `subscribe()` (or `useSubscribe` in React).

Whenever the data in Replicache changes — either due to changes in this tab, another tab, or on the server — the affected UI automatically updates.

Replicache only refires subscriptions when the query results have actually changed, eliminating wasteful re-renders.

## ③ Optimistic Local Mutations

<Tabs
groupId="lang"
defaultValue="js"
values={[
{label: 'JS / React', value: 'js'},
]}>
<TabItem value="js">

```js
const rep = new Replicache({
  name: 'todo-user-id',
  mutators: {
    createTodo: async (tx, {id, text, complete}) => {
      await tx.put(`/todo/${id}`, { text, complete });
    },
  }
});

// createTodo is asynchronous but subscriptions refire immediately.
// You can treat it as if it's in-memory. You don't need to await.
rep.mutate.createTodo(
  id: uuid(),
  text: this.state.todoText,
  complete: false,
});
```

  </TabItem>
</Tabs>

To make changes to the Client View register a _mutator_ with the `Replicache` constructor.

A mutator is a named JavaScript function with JSON-serializable arguments. Replicache executes mutators immediately (aka "optimistically") against the local cache, without waiting for server confirmation. Subscriptions re-fire instantly, and views are updated with the pending change.

Once executed locally, the mutator invocation -- _mutation_ -- is queued to be pushed to and executed by the server in the background. It doesn't matter if the local mutator computes the same result as server later will — Replicache unwinds the effects of the local mutation as soon as the effects of the remote mutation are known.

## ④ Push

<Tabs
groupId="lang"
defaultValue="request"
values={[
{label: 'Request', value: 'request'},
{label: 'Response', value: 'response'},
]}>
<TabItem value="request">

```http
POST /replicache-batch HTTP/2
Host: myservice.com

{
  "clientID": "CB94867E-94B7-48F3-A3C1-287871E1F7FD",
  "mutations": [
    {
      "id": 7,
      "name": "createTodo",
      "args": {
        "id": "AE2E880D-C4BD-473A-B5E0-29A4A9965EE9",
        "title": "Fix the car",
        "complete": false
      }
    },
    {
      "id": 8,
      "name": "toggleComplete",
      "args": {
        "id": "5C2F21E8-A9CC-4DA8-91D6-97D2D1F7CECF",
        "done": true
      }
    }
  ]
}
```

  </TabItem>
  <TabItem value="response">

```http
HTTP/2 200
```

  </TabItem>
</Tabs>

We call the set of local mutations that have not yet been confirmed by the server "pending mutations". Batches of pending mutations are pushed to the `/replicache-push` endpoint on your service as connectivity allows. The push endpoint has an implementation of each mutator and applies each mutation in order to its datastore. The outcome on the server might be different than the outcome of the local mutation that ran against Replicache in the client. That's OK -- the server is authoritative so the Replicache client will converge on the state of the server.

The mechanism by which a mutation is "confirmed" by the server is the client's _last mutation id_. Each mutation is assigned by the client a unique incrementing integer, its _mutation id_. When the server push endpoint applies a mutation, it must transactionally update the `lastMutationID` for the client to that mutation's ID. The next time the client pulls, the server returns the `lastMutationID` for the client, which the client uses to determine which pending mutations have been confirmed. Confirmed pending mutations do not need to be replayed on top of any new state returned by pull, but unconfirmed pending mutations do need to be replayed (see below).

## ⑤,⑥ Poke, Incremental Pull

```js
const pusher = new Pusher({
  appId: 'wwwww',
  key: 'xxxxxxxxxxxxxxxx',
  secret: 'yyyyyyyyyyyyyyyyy',
  cluster: 'mt1',
  useTLS: true,
});

await pusher.trigger(`todos-${userID}`, 'poke', {});
```

After applying a mutation on your server, send a WebSocket "poke" (a message with no payload) hinting to any potentially affected users' devices to try to pull. You can use any WebSocket library or even a hosted service to send this poke. No user data is sent over the web socket — its only purpose is a hint to get the relevant clients to pull soon.

Note that Replicache can also pull on an interval, in addition to or instead of in response to a poke. See [ReplicacheOptions](https://doc.replicache.dev/api/interfaces/ReplicacheOptions) [pullInterval](https://doc.replicache.dev/api/interfaces/ReplicacheOptions#pullInterval).

## ⑦ Rebase

When the Replicache client pulls a new Client View from the server, it potentially needs to replay pending local mutations on top of the new state. The set of pending mutations to replay is the set with mutation id > `lastMutationID` returned by the server in the pull.

To replay, Replicache rewinds the state of the key/value store to the point before the latest pull, applies the changes from the server (from the incremental pull), and then replays unconfirmed pending mutations if any on top.

The new state is revealed to the UI atomically, subscriptions re-fire, and the UI refreshes.
