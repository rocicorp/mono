---
title: Todo, Three Ways
slug: /examples/todo
---

# Todo, Three Ways

This page contains several different implementations of the same simple todo app, demonstrating different ways to build a Replicache app.

You can study them as an example of how to use a particular technique, or just clone them to play with a complete working app, before diving into building your own server.

<p>
  <img src="/img/setup/todo.webp" width="650"/>
</p>

<div style={{float:"left", width:"50%", marginTop: '2.5em', boxSizing:'border-box'}}>
  <h3>todo-nextjs</h3>
  <div style={{marginBottom: '1em'}}>
    <a style={{marginRight:'2ex'}} href="https://replicache-todo-nextjs.vercel.app/">Live Demo</a>
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
<div style={{float:"left", width:"50%", marginTop: '2.5em', paddingLeft:'2ex', boxSizing:'border-box'}}>
  <h3>todo-wc</h3>
  <div style={{marginBottom: '1em'}}>
    <a style={{marginRight:'2ex'}} href="https://todo.onrender.com/">Live Demo</a>
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

<div style={{float:"left", clear: "left", width:"50%", boxSizing:'border-box', marginTop: '2em'}}>
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
