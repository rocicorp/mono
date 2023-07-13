---
title: Offline Operation
slug: /concepts/offline
---

Replicache features robust support for offline operation. Specifically:

1. A tab can go offline and continue to operate for hours to days, then sync up smoothly when it reconnects.
2. Replicache's offline support is "local-first": Replicache reads and writes to local state before the network, meaning that it smoothly transmits online, offline, or slow/flaky networks.
3. Changes sync across tabs in the same browser profile, even while offline.
4. If your application has a way to start while offline (ie Service Worker, or Electron shell), you can start it and see changes made in a previous session.

Note that the potential for serious conflicts grows the longer users are disconnected from each other. While Replicache will converge all clients to the same state, it won't always produce a resolution users would be happy with.

If you intend for your application to be used for long periods of intensive offline use, we recommend implementing a concept of history so that users can undo merges that had unexpected results. [Contact Us](https://replicache.dev/#contact) if you would like help thinking through how to do this.
