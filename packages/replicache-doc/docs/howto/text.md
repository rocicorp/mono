---
title: Edit Text Collaboratively
slug: /howto/text
---

You can implement collaborative text elements within a Replicache applications by sending [Yjs](https://github.com/yjs/yjs) documents over push and pull. This works fairly well. It's easy to send just deltas upstream via Replicache mutations. For downstream, sending just deltas is more difficult. Current users we are aware of just send the whole document which is fine for all but the largest documents. See [`replicache-yjs`](https://github.com/rocicorp/replicache-yjs) for a small example of this.

Many applications can also get by without a full collaborative editing solution if their text is highly structured (e.g., like Notion).

We do plan to offer first-class collaborative text in the future.
