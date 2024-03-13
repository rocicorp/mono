---
title: Setup
slug: /byob/setup
---

Replicache is framework agnostic, and you can use most any libraries and frameworks you like.

We're going to use [Rocicorp's BYOB starter](https://github.com/rocicorp/byob-starter). This is an example of a standard [Express](https://expressjs.com/), [Vite](https://vitejs.dev/) web application.

Clone the BYOB Starter

```bash
git clone git@github.com:rocicorp/byob-starter.git
cd byob-starter
```

Install the Dependencies

```bash
npm install
```

This repository establishes a mono-repository, divided into three distinct workspaces: `client`, `server`, and `shared`. The `client` workspace houses the client-side interface, developed with [Vite](https://vitejs.dev/) for scaffolding and [React](https://react.dev/) for the user interface construction. The `server` workspace contains the server-side logic, implemented using [Express](https://expressjs.com/). The `shared` workspace serves as a central repository for types and classes that are utilized jointly by both the client view and server.

## Next

Next, we'll [design our Client View](./design-client-view.md) – the schema for our client-side data.
