---
title: Offline Operation
slug: /offline
---

Replicache is _resilient_ to the application going offline, but is not designed to start offline or to operate offline for arbitrarily long periods of time.

Functionally:

- A tab can go offline and continue to operate for hours to days, depending on the application. The limiting factor will be size of the HTTP request that Replicache can successfully send when it comes back online, so roughly 4MB of mutations for most serverless platforms.
- If a tab has been used while offline and then is closed, or if the browser crashes, changes from that tab will be pushed to the server the next time Replicache is online. These offline changes will be visible to other tabs only once they have made the transit to the server and are pulled down by the other tabs.
- For the same reason as above, if a tab is closed while offline then a new tab is started while still offline, the new tab will not see changes the first tab made. We do not recommend / support using Replicache with apps that can be started fully offline (e.g., via service worker).
- While offline, changes in one tab are not reflected in other tabs; the tabs will "catch up" with each other when back online.

In practice most modern web applications are not intended to be used for long periods offline and can’t start up offline anyway. Replicache optimizes for online performance, but is resilient against short periods of network loss. If your application is intended to transit periods of offline you should [monitor when you have unsaved changes](faq.md#unpushed) and prompt the user before closing.
