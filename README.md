# Replicache JS SDK

### Offline-First for Every Application

![Node.js CI](https://github.com/rocicorp/replicache-sdk-js/workflows/Node.js%20CI/badge.svg)

## 👋 Quickstart

This tutorial walks through creating a basic offline-first todo app with [Replicache](https://replicache.dev/). If you have any problems or questions, please [join us on Slack](https://join.slack.com/t/rocicorp/shared_invite/zt-h8ygwu8j-RVniv5XsBps0Q9oJXdMyoA). We'd be happy to help.

**Note:** This document assumes you already know what Replicache is, why you might need it, and broadly how it works. If that's not true check out the [design document](https://github.com/rocicorp/replicache/blob/master/design.md) for a detailed deep-dive.

## 🏃‍♂️ Install

```
npm install replicache
```

## 🚴🏿‍♀️ Instantiate

Replicache ships with both ES6 and CommonJS modules. For simplicity, these examples use ES6.

```html
<script type='module'>
    import Replicache from './node_modules/replicache/out/mod.js';

    var rep = new Replicache({
        // URL of the diff server to use. The diff server periodically fetches
        // the "client view" from your service and forwards any delta to the
        // client. You can use our hosted diff server (as here) or a local diff
        // server, which is useful during development. See
        // https://github.com/rocicorp/replicache#server-side for more
        // information on setting up your client view.
        diffServerURL: 'https://serve.replicache.dev/pull',
        
        // Auth token for the diff server, if any.
        diffServerAuth: '1',

        // URL of your service's Replicache batch endpoint. Replicache
        // will send batches of mutations here for application.
        batchURL: 'https://replicache-sample-todo.now.sh/serve/replicache-batch',

        // Auth token for your client view and batch endpoints, if any.
        dataLayerAuth: '2',
    });
</script>
```

## 🚗 Render UI

Use `subscribe()` to open standing queries. Replicache fires `onData` whenever the result of the query changes, either because of local changes or sync.

```js
rep.subscribe(async tx => {
    return await toArray(tx.scan({ prefix: '/todo/' }));
}, {
    onData: result => {
        // Using lit-html, but the principle is the same in any UI framework.
        // See https://github.com/rocicorp/replicache-sdk-js/tree/master/sample/cal
        // for an example using React.
        const toggle = complete => html`<td><input type=checkbox checked=${complete}/></td>`;
        const title = text => html`<td>${text}</td>`;
        const row = todo => html`<tr>${toggle(todo.complete)}${title(todo.text)}</tr>`;
        return html`<table>${data.map(row)}</table>`;
    },
});
```

## 🏎 Mutate Data

Register client-side *mutators* using `register()`.

Mutators run completely locally, without waiting on the server — online, offline, whatever! A record of the mutation is queued and sent to your service's batch endpoint when possible.

Replicache also invokes mutators itself, during sync, to replay unacknowledged changes on top of newly received server state.

```js
const updateTodo = rep.register('updateTodo', async (tx, { id, complete }) => {
    const key = `/todo/${id}`;
    const todo = await tx.get(key);
    todo.complete = complete;
    await tx.put(key, todo);
});

const handleCheckbox = async (id, e) => {
    await updateTodo({ id, complete: e.srcElement.checked });
}
```

## 🚀 Next Steps

That's it! You've built a fully-functioning offline-first todo app against our sample backend. What will you do next?

* [Learn how to build your own backend integration](https://github.com/rocicorp/replicache#server-side)
* [Check out the richer React/Babel/GCal sample](https://github.com/rocicorp/replicache-sdk-js/tree/master/sample/cal)
* [Browse the full JS documentation](https://replicache-sdk-js.now.sh/)
