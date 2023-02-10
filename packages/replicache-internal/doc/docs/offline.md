---
title: Offline
slug: /offline
---

Replicache is _resilient_ to the application going offline, but is not designed to start offline or to operate offline for arbitrarily long periods of time.

Functionally:

- A tab can go offline and continue to operate for hours to days, depending on the application. The limiting factor will be size of the HTTP request that Replicache can successfully send when it comes back online, so roughly 4MB of mutations for most serverless platforms.
- If a tab has been used while offline and then is closed, or if the browser crashes, changes from that tab will be pushed to the server the next time Replicache is online. These offline changes will be visible to other tabs once they have made the transit to the server and are pulled down by the other tabs.
- New tabs to the application cannot be opened while offline (just like regular webapps).
- While offline, changes in one tab are not reflected in other tabs; the tabs will "catch up" with each other when back online.

In practice most modern web applications are not intended to be used for long periods offline and can’t start up offline anyway. Replicache optimizes for online performance, but is resilient against short periods of network loss.
