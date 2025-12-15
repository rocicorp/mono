---
title: Unit Test
slug: /howto/unit-test
---

You can unit test your application directly against Replicache, without having to mock out Replicache's interface. To do so, there are a few considerations:

- You'll need to run your tests in a web environment like [`web-test-runner`](https://modern-web.dev/docs/test-runner/overview/), because Replicache has DOM dependencies.
- You'll want to disable sync. You can do this with any of:
  - Set [`pullURL`](/api/classes/Replicache#pullurl) and [`pushURL`](/api/classes/Replicache#pullurl) to `undefined`. These are read/write so clearing them prevents next push/pull.
  - Set a large delay: setting a large [`pushDelay`](/api/classes/Replicache#pushdelay) will prevent automatically pushing after a mutation. Setting [`pullInterval`](/api/classes/Replicache#pullinterval) will increase the time to the next pull.
  - You could implement a custom [`puller`](/api/classes/Replicache#puller)/[`pusher`](/api/classes/Replicache#pusher).
- You may want to run Replicache in-memory. This can be done by setting the [kvStore](/api/interfaces/ReplicacheOptions#kvstore) parameter to `mem`.
  - Alternately, you can keep using the persistent storage and pick a randomly-generated `name` for your Replicache instance each time you create it.
