---
title: Frequently Asked Questions
slug: /faq
---

import TOCInline from '@theme/TOCInline';

<TOCInline toc={toc} />

## How does the client know when to sync? Does it poll?

Typically, servers send a _poke_ (a content-less hint) over a websocket to tell the client to sync. There are many services you can use for this, and since no content flows over the socket there is no security/privacy concern. See the [integration guide](/guide/poke) for more information.

Replicache also polls at a low interval (60s by default) in case the poke mechanism fails or for applications that don't require low latency updates. You can adjust this using the [`pullInterval`](api/interfaces/ReplicacheOptions#pullInterval) field.

## What if I don’t have a dedicated backend? I use serverless functions for my backend

No problem. You can implement the integration points as serverless functions. Our samples are all implemented this way.

## How can I programmatically prevent Replicache from syncing?

Options:

- Set `pullURL` and `pushURL` to `undefined`. These are read/write so clearing them prevents next push/pull.
- Set a large delay: setting a large `pushDelay` will prevent automatically pushing after a mutation. Setting `pullInterval` will increase the time to the next pull.
- You could implement a custom `puller`/`pusher`.

If you would like better / more first-class support for this please [file an issue](https://github.com/rocicorp/replicache/issues/new).

## How can I tell if Replicache has unpushed local mutations? {#unpushed}

Replicache doesn't currently have first-class support for this. It is possible to implement an "unconfirmed changes" monitor using the Client View, by keeping your own mutation sequence number and having the server include its high-water mark in the Client View. If you would like better / more first-class support for this please [file an issue](https://github.com/rocicorp/replicache/issues/new).

## Do you support collaborative text editing?

We don't have first-class support for collaborative-text yet.

Some users implement collaborative text elements within a Replicache applications by sending [Yjs](https://github.com/yjs/yjs) documents over push and pull. This works fairly well. It's easy to send just deltas upstream via Replicache mutations. For downstream, sending just deltas is more difficult. Current users we are aware of just send the whole document which is fine for smaller documents.

Many applications can also get by without a full collaborative editing solution if their text is highly structured (e.g., like Notion).

We do plan to offer first-class collaborative text in the future.

## How can I implement presence?

Replicache is a general purpose synchronization system. It doesn't have a first-class concept of presence, but it is easy to build one that works exactly how you want. See [Replidraw](https://github.com/rocicorp/replidraw) for an example.
