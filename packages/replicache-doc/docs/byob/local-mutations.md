---
title: Local Mutations
slug: /byob/local-mutations
---

With Replicache, you implement mutations once on the client-side (sometimes called _speculative_ or _optimistic_ mutations), and then again on the server (called _authoritative_ mutations).

:::note info

The two implementations need not match exactly. Replicache replaces the result of a speculative change completely with the result of the corresponding authoritative change, once it's known. This is useful because it means the speculative implementation can frequently be pretty simple, not taking into account security, complex business logic edge cases, etc.

:::

First, let's register a _mutator_ that speculatively creates a message. In `index.tsx`, expand the options passed to the `Replicache` constructor with:

```js {4-15}
//...
const r = new Replicache({
  name: 'chat-user-id',
  mutators: {
    async createMessage(
      tx: WriteTransaction,
      {id, from, content, order}: MessageWithID,
    ) {
      await tx.set(`message/${id}`, {
        from,
        content,
        order,
      });
    },
  },
  pushURL: `/api/replicache/push`,
  pullURL: `/api/replicache/pull`,
  logLevel: 'debug',
});
//...
```

When invoked, the implementation is run within a transaction (`tx`) and it `put`s the new message into the local map.

Now let's invoke the mutator when the user types a message. Replace the content of `onSubmit` so that it invokes the mutator:

```js
    const onSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
      e.preventDefault();
      let last: Message | null = null;
      if (messages.length) {
        const lastMessageTuple = messages[messages.length - 1];
        last = lastMessageTuple[1];
      }
      const order = (last?.order ?? 0) + 1;
      const username = usernameRef.current?.value ?? '';
      const content = contentRef.current?.value ?? '';

      await r?.mutate.createMessage({
        id: nanoid(),
        from: username,
        content,
        order,
      });

      if (contentRef.current) {
        contentRef.current.value = '';
      }
    };
```

Previously we mentioned that Replicache has a mechanism that ensures that local, speculative changes are always applied on top of changes from the server. The way this works is that when Replicache pulls and applies changes from the server, any mutator invocations that have not yet been confirmed by the server are _replayed_ on top of the new server state. This is much like a git rebase, and the effects of the patch-and-replay are revealed atomically to your app.

An important consequence of this is that unique IDs should often be passed into mutators as parameters, and not generated inside the mutator. This may be counter-intuitive at first, but it makes sense when you remember that Replicache is going to replay this transaction during sync, and we don't want the ID to change!

:::note info

Careful readers may be wondering what happens with the order field during sync. Can multiple messages end up with the same order? Yes! But in this case, what the user likely wants is for their message to stay roughly at the same position in the stream, and using the client-specified order and sorting by that roughly achieves the desired result. If we wanted better control over this, we could use [fractional indexing](https://www.npmjs.com/package/fractional-indexing) but that's not necessary in this case.

:::

Restart the server and you should now be able to make changes. Note that changes are already propagating between tabs, even though we haven't done anything on the server yet. And this works even if you kill the server. This is because Replicache stores data locally that is shared between all tabs in a browser profile.

<p class="text--center">
  <img src="/img/setup/local-mutation.gif" width="650"/>
</p>

## Next

That's actually it for the client! Next, we'll start work on our server by [setting up a remote database](./database-setup.md).
