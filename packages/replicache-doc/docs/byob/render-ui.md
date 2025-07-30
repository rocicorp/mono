---
title: Render UI
slug: /byob/render-ui
---

The next step is to use the data in the Client View to render your UI.

First, let's define a few simple types. Replicache supports strongly-typed mutators – we'll use these types later to ensure our UI passes the correct data. Modify the `types.ts` at `shared/src/types.ts`

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
/* eslint-disable @typescript-eslint/no-unused-vars */
import React, {useEffect, useRef, useState} from 'react';
import ReactDOM from 'react-dom/client';
import {Replicache, WriteTransaction} from 'replicache';
import {Message, MessageWithID} from 'shared';
import {useSubscribe} from 'replicache-react';
import Pusher from 'pusher-js';
import {nanoid} from 'nanoid';

async function init() {
  function Root() {
    const [r, setR] = useState<Replicache<any> | null>(null);

    useEffect(() => {
      console.log('updating replicache');
      const r = new Replicache({
        name: 'chat-user-id',
        pushURL: `/api/replicache/push`,
        pullURL: `/api/replicache/pull`,
        logLevel: 'debug',
      });
      setR(r);
      listen(r);
      return () => {
        void r.close();
      };
    }, []);

    const messages = useSubscribe(
      r,
      async tx => {
        const list = await tx
          .scan<Message>({prefix: 'message/'})
          .entries()
          .toArray();
        list.sort(([, {order: a}], [, {order: b}]) => a - b);
        return list;
      },
      {default: []},
    );

    const usernameRef = useRef<HTMLInputElement>(null);
    const contentRef = useRef<HTMLInputElement>(null);

    const onSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
      e.preventDefault();
      // TODO: Create Message
    };

    return (
      <div>
        <form onSubmit={onSubmit}>
          <input ref={usernameRef} required /> says:
          <input ref={contentRef} required /> <input type="submit" />
        </form>
        {messages.map(([k, v]) => (
          <div key={k}>
            <b>{v.from}: </b>
            {v.content}
          </div>
        ))}
      </div>
    );
  }

  ReactDOM.createRoot(document.getElementById('root') as HTMLElement).render(
    <React.StrictMode>
      <Root />
    </React.StrictMode>,
  );
}

function listen(rep: Replicache) {
  // TODO: Listen for changes on server
}

await init();
```

Navigate to [http://localhost:5173/](http://localhost:5173). You should see that we're rendering data from Replicache!

This might not seem that exciting yet, but notice that if you change `replicache/pull` temporarily to return 500 (or remove it, or cause any other error, or just make it really slow), the page still renders instantly.

That's because we're rendering the data from the local cache on startup, not waiting for the server! Woo.

## Next

Enough with static data. The next section adds [local mutations](./local-mutations.md), which is how we implement optimistic UI in Replicache.
