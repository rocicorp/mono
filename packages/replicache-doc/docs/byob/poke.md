---
title: Poke
slug: /byob/poke
---

By default, Replicache pulls new changes periodically. The frequency is controlled by the [`pullInterval`](/api/interfaces/ReplicacheOptions#pullinterval) parameter which defaults to 60 seconds.

To get more responsive updates, you could reduce the pull interval, but that gets expensive quick. Most Replicache applications instead have the server send a special message called a _poke_ to the app, telling it when it should pull again.

A Replicache poke caries no data – it's only a hint telling the client to pull soon. This enables developers to build their realtime apps in the standard stateless request/response style. You can even build Replicache-enabled apps serverlessly (as we are here with Next.js)!

Because pokes are simple, you can implement them many ways. Any hosted WebSocket service like [Pusher](https://pusher.com/) or [PubNub](https://www.pubnub.com/) works. You can also implement your own WebSocket server or use server-sent events. And some databases come with features that can be used for pokes. For several different examples to implementing pokes, see [Todo, Three Ways](/examples/todo).

For this sample, we'll use Pusher. Go to [pusher.com](https://pusher.com) and setup a free "Channels" project with client type "React" and server type "Node.js".

Store the settings from the project in the following environment variables:

```bash
export REPLICHAT_PUSHER_APP_ID=<app id>
export REPLICHAT_PUSHER_KEY=<key>
export REPLICHAT_PUSHER_SECRET=<secret>
export REPLICHAT_PUSHER_CLUSTER=<cluster>
export VITE_PUBLIC_REPLICHAT_PUSHER_KEY=<key>
export VITE_PUBLIC_REPLICHAT_PUSHER_CLUSTER=<cluster>
```

Typically you'll establish one WebSocket _channel_ per-document or whatever the unit of collaboration is in your application. For this simple demo, we just create one channel, `"default"`.

Replace the implementation of `sendPoke()` in `push.ts`:

```ts
import Pusher from 'pusher';
//...
async function sendPoke() {
  if (
    !process.env.REPLICHAT_PUSHER_APP_ID ||
    !process.env.REPLICHAT_PUSHER_KEY ||
    !process.env.REPLICHAT_PUSHER_SECRET ||
    !process.env.REPLICHAT_PUSHER_CLUSTER
  ) {
    throw new Error('Missing Pusher environment variables');
  }
  const pusher = new Pusher({
    appId: process.env.REPLICHAT_PUSHER_APP_ID,
    key: process.env.REPLICHAT_PUSHER_KEY,
    secret: process.env.REPLICHAT_PUSHER_SECRET,
    cluster: process.env.REPLICHAT_PUSHER_CLUSTER,
    useTLS: true,
  });
  const t0 = Date.now();
  await pusher.trigger('default', 'poke', {});
  console.log('Sent poke in', Date.now() - t0);
}
```

Then on the client, in `client/src/index.tsx`, replace the implementation of `listen()` to tell Replicache to `pull()` whenever a poke is received:

```ts
function listen(rep: Replicache) {
  console.log('listening');
  // Listen for pokes, and pull whenever we get one.
  Pusher.logToConsole = true;
  if (
    !import.meta.env.VITE_PUBLIC_REPLICHAT_PUSHER_KEY ||
    !import.meta.env.VITE_PUBLIC_REPLICHAT_PUSHER_CLUSTER
  ) {
    throw new Error('Missing PUSHER_KEY or PUSHER_CLUSTER in env');
  }
  const pusher = new Pusher(import.meta.env.VITE_PUBLIC_REPLICHAT_PUSHER_KEY, {
    cluster: import.meta.env.VITE_PUBLIC_REPLICHAT_PUSHER_CLUSTER,
  });
  const channel = pusher.subscribe('default');
  channel.bind('poke', async () => {
    console.log('got poked');
    await rep.pull();
  });
}
```

Finally, ensure Pusher and Replicache disconnect at the same time
in `client/src/index.tsx`, replace Replicache useEffect return with

```ts
return () => {
  Pusher.instances.forEach(i => i.disconnect());
  void r.close();
};
```

Restart the app, and make a change, and you should see it propagate live between browsers:

<p class="text--center">
  <img src="/img/setup/sync.webp" width="650"/>
</p>

## Next

And that's it! The [next section](./conclusion.md) wraps up.
