---
title: Poke
slug: /guide/poke
---

Many realtime systems use WebSockets to push live updates to clients. This has performance benefits, but comes at a steep operational cost. WebSocket-based services are more complex to scale because they have to keep state in memory for each connected client.

Replicache instead uses WebSockets only to hint the client that it should pull again. No data is sent over the socket. This enables developers to build their realtime web applications in the standard stateless request/response style. You can even build Replicache-enabled apps serverlessly (as we are here with Next.js)! We have found that we can get most of the performance back with HTTP/2, anyway.

We refer to this WebSocket hint as a _poke_, to go along with _push_ and _pull_. You can use any hosted WebSocket service to send pokes, such as [socket.io](https://socket.io) or [Pusher](https://pusher.com/), and it's trivial to setup.

For this sample, we'll use Pusher. Get thee to [pusher.com](https://pusher.com) and setup a free "Channels" project with client type "React" and server type "Node.js".

Store the settings from the project in the following environment variables:

```bash
export NEXT_PUBLIC_REPLICHAT_PUSHER_APP_ID=<app id>
export NEXT_PUBLIC_REPLICHAT_PUSHER_KEY=<key>
export NEXT_PUBLIC_REPLICHAT_PUSHER_SECRET=<secret>
export NEXT_PUBLIC_REPLICHAT_PUSHER_CLUSTER=<cluster>
```

Import the library into `pages/api/replicache-push.js`:

```js
import Pusher from 'pusher';
```

Typically you'll establish one WebSocket _channel_ per-document or whatever the unit of collaboration is in your application. For this simple demo, we just create one channel, `"default"`.

Replace the implementation of `sendPoke()` in `replicache-push.js`:

```js
async function sendPoke() {
  const pusher = new Pusher({
    appId: process.env.NEXT_PUBLIC_REPLICHAT_PUSHER_APP_ID,
    key: process.env.NEXT_PUBLIC_REPLICHAT_PUSHER_KEY,
    secret: process.env.NEXT_PUBLIC_REPLICHAT_PUSHER_SECRET,
    cluster: process.env.NEXT_PUBLIC_REPLICHAT_PUSHER_CLUSTER,
    useTLS: true,
  });
  const t0 = Date.now();
  await pusher.trigger('default', 'poke', {});
  console.log('Sent poke in', Date.now() - t0);
}
```

Then on the client, in `index.js`, import the client library:

```js
import Pusher from 'pusher-js';
```

... and replace the implementation of `listen()` to tell Replicache to `pull()` whenever a poke is received:

```js
function listen(rep) {
  console.log('listening');
  // Listen for pokes, and pull whenever we get one.
  Pusher.logToConsole = true;
  const pusher = new Pusher(process.env.NEXT_PUBLIC_REPLICHAT_PUSHER_KEY, {
    cluster: process.env.NEXT_PUBLIC_REPLICHAT_PUSHER_CLUSTER,
  });
  const channel = pusher.subscribe('default');
  channel.bind('poke', () => {
    console.log('got poked');
    rep.pull();
  });
}
```

Restart the app, and make a change, and you should see it propagate live between browsers:

<p class="text--center">
  <img src="/img/setup/sync.webp" width="650"/>
</p>
