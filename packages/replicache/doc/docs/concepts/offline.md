---
title: Offline Operation
slug: /concepts/offline
---

Replicache is _resilient_ to the application going offline, but does not currently support starting offline or operating offline for arbitrarily long periods of time.

Specifically:

1. A tab can go offline and continue to operate for hours to days, depending on the application. The limiting factor will typically be size of the HTTP request that Replicache can successfully send when it comes back online (so ~4MB of mutations on most serverless platforms and maybe 50MB or more on serverful platforms).
2. If a tab has been used while offline and then is closed, or if the browser crashes, changes from that tab will be pushed to the server the next time Replicache is online. These offline changes will be visible to other tabs only once they have made the transit to the server and are pulled down by the other tabs.
3. For the same reason as above, if a tab is closed while offline then a new tab is started while still offline, the new tab will not see changes the first tab made. We do not recommend / support using Replicache with apps that can be started fully offline (e.g., via service worker).
4. While offline, changes in one tab are not reflected in other tabs; the tabs will "catch up" with each other when back online.

We are working on improving our offline support, and expect to remove the above caveats in the near-future. If you are interested in learning more, [Contact Us](https://replicache.dev/#contact).
