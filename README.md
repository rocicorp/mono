# Replicache JS SDK

### Offline-First for Every Application

![Node.js CI](https://github.com/rocicorp/replicache-sdk-js/workflows/Node.js%20CI/badge.svg)

## 👋 Quickstart

This tutorial walks through creating the UI for a basic [Replicache](https://replicache.dev/)-powered todo list.

It relies on the [replicache-sample-todo](https://github.com/rocicorp/replicache-sample-todo) backend. To learn how to setup you own Replicache backend, see [Server Side Setup](https://github.com/rocicorp/replicache/blob/main/SERVER_SETUP.md).

If you have any problems or questions, please [join us on Slack](https://slack.replicache.dev/). We'd be happy to help.

You can also skip to the end and [check out the full working version of this sample](https://github.com/rocicorp/replicache-sdk-js/tree/main/sample/cal).

**Note:** This document assumes you already know what Replicache is, why you might need it, and broadly how it works. If that's not true check out the [design document](https://github.com/rocicorp/replicache/blob/main/design.md) for a detailed deep-dive.

## 🏃‍♂️ Install

```
npm install replicache
```

## 🚴🏿‍♀️ Instantiate

```html
<script type="module">
  import Replicache from 'replicache'; // Replace with a real module path as needed...

  var rep = new Replicache({
    // URL of the diff server to use. The diff server periodically fetches
    // the "client view" from your service and forwards any delta to the
    // client. You can use our hosted diff server (as here) or a local diff
    // server, which is useful during development. See
    // https://github.com/rocicorp/replicache/blob/main/SERVER_SETUP.md for more
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

Use `subscribe()` to open standing queries. Replicache calls `onData` whenever the result of the query changes, either because of local changes or sync.

```js
rep.subscribe(
  async tx => {
    return await toArray(tx.scan({prefix: '/todo/'}));
  },
  {
    onData: result => {
      // Using lit-html, but the principle is the same in any UI framework.
      // See https://github.com/rocicorp/replicache-sdk-js/tree/main/sample/cal
      // for an example using React.
      const toggle = complete =>
        html`<td><input type="checkbox" .checked=${complete} /></td>`;
      const title = text => html`<td>${text}</td>`;
      const row = todo =>
        html`<tr>
          ${toggle(todo.complete)}${title(todo.text)}
        </tr>`;
      render(
        html`<table>
          ${result.map(row)}
        </table>`,
        document.body,
      );
    },
  },
);
```

## 🏎 Mutate Data

Register client-side _mutators_ using `register()`.

Mutators run completely locally, without waiting on the server — online, offline, whatever! A record of the mutation is queued and sent to your service's batch endpoint when possible.

Replicache also invokes mutators itself, during sync, to replay unacknowledged changes on top of newly received server state.

```js
const updateTodo = rep.register('updateTodo', async (tx, {id, complete}) => {
  const key = `/todo/${id}`;
  const todo = await tx.get(key);
  todo.complete = complete;
  await tx.put(key, todo);
});

const handleCheckbox = async (id, e) => {
  await updateTodo({id, complete: e.srcElement.checked});
};
```

## 🛫 Tips

- We recommend [enabling console persistence](https://stackoverflow.com/questions/5327955/how-to-make-google-chrome-javascript-console-persistent) while developing replicache-enabled apps to make debugging easier.
- Remember that data changes can happen "underneath" you and cause `subscribe()` to re-fire at any time. These changes can come from the server or from a different tab. If your UI is not reactive (driven solely by the data model) you need to take extra steps to ensure the UI is in sync with the data.

## 🚀 Next Steps

That's it! You've built a fully-functioning offline-first todo app against our sample backend. What will you do next?

- [Check out the full version of this sample](https://github.com/rocicorp/replicache-sdk-js/tree/main/sample/lit-todo)
- [Learn how to add Replicache support to your own backend service](https://github.com/rocicorp/replicache/blob/main/SERVER_SETUP.md)
- [Check out the richer React/Babel/GCal sample](https://github.com/rocicorp/replicache-sdk-js/tree/main/sample/cal)
- [Browse the full JS documentation](https://replicache-sdk-js.now.sh/)
