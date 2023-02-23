---
title: Adding Mutators
slug: /tutorial/adding-mutators
---

<iframe src="https://codesandbox.io/embed/replicache-mutators-jvqxpb?autoresize=1&fontsize=12&hidenavigation=1&theme=light&codemirror=1&view=split"
     style={{'width':'100%','height':'350px', 'border':'1px solid rgb(222,221,221)', 'overflow':'hidden'}}
     title="constructing-replicache"
     allow="accelerometer; ambient-light-sensor; camera; encrypted-media; geolocation; gyroscope; hid; microphone; midi; payment; usb; vr; xr-spatial-tracking"
     sandbox="allow-forms allow-modals allow-popups allow-presentation allow-same-origin allow-scripts"
></iframe>

_Mutators_ are how you change data in Replicache.

Mutators are arbitrary functions that run once on the client immediately (aka “optimistically”), **and then run again later on the server** (”authoritatively”) during sync.

Replicache queues mutations locally until the server acknowledges them during sync. Once the authoritative server result is known, Replicache reverts the optimistic version completely. For a deeper understanding of how the authoritative server works please read about [synchronization](/concepts/how-it-works#sync-details).

:::tip Mutators are fast

Although the methods of `tx` are marked `async`, they almost always responds instantly (in the same event, < 1ms after call). The only reason access is async is for the rare case when Replicache must load data from local storage, such as at startup.

Replicache is designed to be memory-fast and you should not need additional layers of caching above it. See [performance](/concepts/performance) for more information.

:::

<h2>Challenge</h2>

Try adding your own multiply mutator.

<div style={{fontSize:"1.2em", fontWeight:"bold", marginTop:"3em"}}><a href="/tutorial/subscriptions">Next: Subscriptions &rarr;</a></div>
