---
title: Features
slug: /app-features
---

This simple quickstart demo has some powerful features under the covers:

- Open the web inspector in one window and throttle the network. Notice that the UI still responds instantly.
- Open the web inspector in one window and completely disable the network. When the network comes back, everything syncs up!
- Disable the network and engineer a conflict. For example, delete a todo in the online tab, and edit the same todo in the offline tab. When the offline tab comes back both todos will be deleted.

The thing to understand is that **you do not have to write _any_ code to get these behaviors**. You write your Replicache app almost entirely client-side, and you get:

- Optimistic mutations everywhere, automatically.
- Rollback and reconciliaton when server mutation is different than optimistic.
- Instant UI, even when network is slow.
- Offline support.

# Next

The [next sections](/app-structure) walk you through the basic structure of this starter app, and explain how Replicache provides these benefits.
