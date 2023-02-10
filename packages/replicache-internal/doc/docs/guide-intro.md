---
title: Introduction
slug: /guide/intro
---

This integration guide walks you through the steps required to build a complete Replicache app, including the backend, from scratch.

The main reasons to do this would be:

- You want to integrate Replicache into some existing backend
- You want to build a Replicache backend in some language other than JavaScript, so you can't use the starter app
- You just like knowing how things work

:::note

Building a Replicache backend isn't _super_ hard, but there is some subtlety to getting push and pull correct and efficient. As such, we generally recommend users start with the [replicache-todo starter app](../examples/todo) if possible and fork it to taste. Easier to start with something that already works!

:::

<p class="text--center">
  <img src="/img/setup/sync.webp" width="650"/>
</p>

You can follow the steps exactly to end up with a simple chat app, or use them as guide to build your own Replicache-enabled app.

## Prerequisites

You only need [Node.js](https://nodejs.org/en/), version 10.13 or higher to get started.

You should also already understand [How Replicache Works](../how-it-works) at a high level.
