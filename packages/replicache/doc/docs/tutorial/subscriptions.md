---
title: Subscriptions
slug: /tutorial/subscriptions
---

<iframe src="https://codesandbox.io/embed/replicache-subscriptions-13-s5tdqk?autoresize=1&fontsize=12&hidenavigation=1&theme=light&codemirror=1&view=split"
     style={{'width':'100%','height':'350px', 'border':'1px solid rgb(222,221,221)', 'overflow':'hidden'}}
     title="constructing-replicache"
     allow="accelerometer; ambient-light-sensor; camera; encrypted-media; geolocation; gyroscope; hid; microphone; midi; payment; usb; vr; xr-spatial-tracking"
     sandbox="allow-forms allow-modals allow-popups allow-presentation allow-same-origin allow-scripts"
></iframe>

_Subscriptions_ work similarly to other reactive JS frameworks.

You can subscribe to a _query_ of Replicache and you will get notified when that query changes for any reason — either because of local optimistic changes, or because of sync.

:::tip Subscriptions are Fast

Replicache goes to significant trouble to make reactive renders efficient:

- Replicache only calls the query function (the parameter to subscribe) when any of the keys it accessed last time change.
- The `onData` callback is only called when the result of the query function changes.
- Replicache will usually return objects with the same identity across queries, so you can use things like React’s `useMemo` to avoid re-renders.

:::

<h2>Challenge</h2>

Modify the sample to increment by zero, and verify that the `onData` callback is not called. This is because even though the `count` key was re-written, its value didn't change. So Replicache didn't call the `onData` callback.

<div style={{fontSize:"1.2em", fontWeight:"bold", marginTop:"3em"}}><a href="/tutorial/sync">Next: Sync &rarr;</a></div>
