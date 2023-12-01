---
title: Render UI
slug: /byob/render-ui
---

The next step is to use the data in the Client View to render your UI.

First, let's define a few simple types. Replicache supports strongly-typed mutators – we'll use these types later to ensure our UI passes the correct data. Create a new `types.ts` at the root and add this code:

```ts
export type Message = {
  from: string;
  content: string;
  order: number;
};

export type MessageWithID = Message & {id: string};
```

Now we'll build the UI. The model is that the view is a [pure function](https://en.wikipedia.org/wiki/Pure_function) of the data in Replicache. Whenever the data in Replicache changes — either due to local mutations or syncing with the server — subscriptions will fire, and your UI components re-render. Easy.

To create a subscription, use the `useSubscribe()` React hook. You can do multiple reads and compute a result. Your React component only re-renders when the returned result changes.

Let's use a subscription to implement our chat UI. Replace `index.tsx` with the below code:

```tsx
import React, { FormEvent, useRef } from "react";
import { Replicache, TEST_LICENSE_KEY, WriteTransaction } from "replicache";
import { useSubscribe } from "replicache-react";
import { nanoid } from "nanoid";
import Pusher from "pusher-js";
import { Message, MessageWithID } from "../types";

const rep = process.browser
  ? new Replicache({
      name: "chat-user-id",
      licenseKey: TEST_LICENSE_KEY,
      pushURL: "/api/replicache-push",
      pullURL: "/api/replicache-pull",
    })
  : null;

listen();

export default function Home() {
  const messages = useSubscribe(
    rep,
    async (tx) => {
      const list = await tx
        .scan<Message>({ prefix: "message/" })
        .entries()
        .toArray();
      list.sort(([, { order: a }], [, { order: b }]) => a - b);
      return list;
    },
    { default: [] }
  );

  const usernameRef = useRef<HTMLInputElement>();
  const contentRef = useRef<HTMLInputElement>();

  const onSubmit = (e) => {
    e.preventDefault();
    // TODO: Create message
  };

  return (
    <div>
      <form onSubmit={onSubmit}>
        <input ref={usernameRef} required /> says:{" "}
        <input ref={contentRef} required /> <input type="submit" />
      </form>
      <MessageList messages={messages} />
    </div>
  );
}

function MessageList({
  messages,
}: {
  messages: (readonly [string, Message])[];
}) {
  return messages.map(([k, v]) => {
    return (
      <div key={k}>
        <b>{v.from}: </b>
        {v.content}
      </div>
    );
  });
}

function listen() {
  // TODO: Listen for changes on server
}
```

Then restart your server and navigate to [http://localhost:3000/](http://localhost:3000). You should see that we're rendering data from Replicache!

<p class="text--center">
  <img src="/img/setup/static-ui.webp" width="650"/>
</p>

This might not seem that exciting yet, but notice that if you change `replicache-pull` temporarily to return 500 (or remove it, or cause any other error, or just make it really slow), the page still renders instantly.

That's because we're rendering the data from the local cache on startup, not waiting for the server! Woo.

## Next

Enough with static data. The next section adds [local mutations](./local-mutations.md), which is how we implement optimistic UI in Replicache.
