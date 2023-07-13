---
title: Sync
slug: /tutorial/sync
---

   <iframe src="https://codesandbox.io/embed/replicache-sync-13-vj6jpn?autoresize=1&fontsize=12&hidenavigation=0&theme=light&codemirror=1&view=split"
     style={{'width':'100%','height':'350px', 'border':'1px solid rgb(222,221,221)', 'overflow':'hidden'}}
     title="constructing-replicache"
     allow="accelerometer; ambient-light-sensor; camera; encrypted-media; geolocation; gyroscope; hid; microphone; midi; payment; usb; vr; xr-spatial-tracking"
     sandbox="allow-forms allow-modals allow-popups allow-presentation allow-same-origin allow-scripts"
></iframe>

Finally, let’s start syncing our changes to the server!

Replicache can sync with any server that implements the Replicache sync protocol. You can learn how to build such a server in the [BYOB Tutorial](/byob/intro). For now, we’ll just connect to an existing server by adding `pushURL` and `pullURL` parameters to the constructor.

**Copy the preview URL (i.e. xxxxx.csb.app/space/123) into a different tab or browser, and click increment to see the two tabs sync.**

:::tip Spaces

What's that `initSpace()` call?

For each run of this demo, we create a new _space_ on the server to store data in. This ensures each visitor to this demo sees only their own counts and isn't confused by seeing other users incrementing the count at the same time.

:::

To support realtime updates, most Replicache server support an optional _poke channel_. Replicache _pokes_ are zero-length messages that serve as a hint from server to clients that a space has changed and that the clients should pull again soon. This sample server implements a poke channel using [Server-Sent Events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events). For a deeper understanding of how poke works please refer to our [poke](/byob/poke) documentation.

<h2>Challenge</h2>

Replicache mutators are not required to compute the same result on the client as the server.

This is a feature! The server can have different or better information than the client. Also, this prevents clients from lying or cheating.

Try modifying your increment mutator to compute an incorrect result. You will see the incorrect result momentarily on the client or for as long as you are offline. But after sync, the clients snaps into alignment with the correct answer from the server automatically.

<div style={{fontSize:"1.2em", fontWeight:"bold", marginTop:"3em"}}><a href="/tutorial/next-steps">Next: Next Steps &rarr;</a></div>
