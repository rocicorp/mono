---
title: Quickstarts
slug: /quickstarts
---

Minimal examples, demonstrating different stacks and techniques for building Replicache apps. You can clone these to easily play with Replicache before diving into building your own server.

## todo-react

One of the most common Replicache setups.

**https://github.com/rocicorp/todo-react**

<table>
  <tr>
    <td>UI</td>
    <td>React</td>
  </tr>
  <tr>
    <td>Server</td>
    <td>Express</td>
  </tr>
  <tr>
    <td>Storage</td>
    <td>Postgres</td>
  </tr>
  <tr>
    <td>Diff Strategy</td>
    <td>Per-Space Version</td>
  </tr>
  <tr>
    <td>Pokes</td>
    <td>Server-Sent Events</td>
  </tr>
</table>

## todo-wc

Demonstrated usage of Replicache with Web Components/Vanilla JS. Otherwise identical to todo-react.

**https://github.com/rocicorp/todo-wc**

<table>
  <tr>
    <td>UI</td>
    <td>Web Components</td>
  </tr>
  <tr>
    <td>Server</td>
    <td>Express</td>
  </tr>
  <tr>
    <td>Storage</td>
    <td>Postgres</td>
  </tr>
  <tr>
    <td>Diff Strategy</td>
    <td>Per-Space Version</td>
  </tr>
  <tr>
    <td>Pokes</td>
    <td>Server-Sent Events</td>
  </tr>
</table>

## todo-nextjs

Demonstrated Replicache with serverless backends, and usage of Supabase for pokes.

**https://github.com/rocicorp/todo-nextjs**

<table>
  <tr>
    <td>UI</td>
    <td>Next.js/React</td>
  </tr>
  <tr>
    <td>Server</td>
    <td>Serverless functions on Vercel</td>
  </tr>
  <tr>
    <td>Storage</td>
    <td>Supabase</td>
  </tr>
  <tr>
    <td>Diff Strategy</td>
    <td>Global Version</td>
  </tr>
  <tr>
    <td>Pokes</td>
    <td>Built on Supabase's realtime support</td>
  </tr>
</table>
