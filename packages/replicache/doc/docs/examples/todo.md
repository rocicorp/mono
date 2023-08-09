---
title: Todo
slug: /examples/todo
---

# Todo, Three Ways

Replicache is very unopinionated. You can build apps using different UI frameworks, databases, and backend stacks.

For example:

- Replicache can support any **frontend** JavaScript UI framework like React, Solid, Web Components, etc.
- Replicache supports both stateful (like Express.js) and stateless/serverless (like Next.js) **backends**. Backends can also be implemented in any programming language.
- If the backend is JavaScript-based, the code for Replicache **mutators** can be shared (see [Share Mutators](/howto/share-mutators)).
- Replicache works with many **databases** (see [Databases](/byob/remote-database)).
- The Replicache server endpoints can be implemented using several different high-level **strategies** (see [Backend Strategies](/strategies/overview)).
- **Pokes** tell Replicache a change has happened on the server and it should fetch updates. They can be implemented with Web Sockets, Server-Sent Events, or third-party services. (See [Poke](/byob/poke)).

This page contains several different implementations of the same simple todo app, each demonstrating different choices for these decisions.

You can study them as an example of how to use a particular technique, or just clone them to play with a complete working app, before diving into building your own server.

<p>
  <img src="/img/setup/todo.webp" width="650"/>
</p>

<div style={{float:"left", width:"50%", marginTop: '2.5em', boxSizing:'border-box'}}>
  <h3>todo-nextjs</h3>
  <div style={{marginBottom: '1em'}}>
    <a style={{marginRight:'2ex'}} href="https://replicache-todo.vercel.app">Live Demo</a>
    <a href="https://github.com/rocicorp/todo-nextjs">Source Code</a>
  </div>
  <p>
    <b>Frontend:</b> Next.js<br/>
    <b>Backend:</b> Next.js/Vercel<br/>
    <b>Mutators:</b> Shared<br/>
    <b>Database:</b> Supabase<br/>
    <b>Strategy:</b> Global Versioning<br/>
    <b>Pokes:</b> Supabase Realtime
  </p>
</div>

<div style={{float:"left", width:"50%", paddingLeft:'2ex', marginTop: '2em', boxSizing:'border-box'}}>
  <h3>todo-wc</h3>
  <div style={{marginBottom: '1em'}}>
    <a style={{marginRight:'2ex'}} href="https://todo-wc.onrender.com/">Live Demo</a>
    <a href="https://github.com/rocicorp/todo-wc">Source Code</a>
  </div>
  <p>
    <b>Frontend:</b> Web Components / Vanilla JS<br/>
    <b>Backend:</b> Node.js/Express<br/>
    <b>Mutators:</b> Shared<br/>
    <b>Database:</b> Postgres<br/>
    <b>Strategy:</b> Per-Space Versioning<br/>
    <b>Pokes:</b> Server-Sent Events
  </p>
</div>

<div style={{float:"left", clear:"left", width:"50%", marginTop: '2.5em', boxSizing:'border-box'}}>
  <h3>todo-row-versioning</h3>
  <div style={{marginBottom: '1em'}}>
    <a style={{marginRight:'2ex'}} href="https://todo-row-versioning.onrender.com/">Live Demo</a>
    <a href="https://github.com/rocicorp/todo-row-versioning">Source Code</a>
  </div>
  <p>
    <b>Frontend:</b> React<br/>
    <b>Backend:</b> Node.js/Express<br/>
    <b>Mutators:</b> Unshared<br/>
    <b>Database:</b> Postgres<br/>
    <b>Strategy:</b> Row Versioning<br/>
    <b>Pokes:</b> Server-Sent Events
  </p>
</div>
