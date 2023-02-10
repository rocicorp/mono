---
title: Deploy to Render
slug: /deploy-render
---

[Render](https://render.com) is a modern Heroku-like service for stateful servers. It's super easy to deploy the quickstart app there.

### Step 1: Create Render Project from Blueprint

- Push your app to GitHub
- [Create a Render account](https://dashboard.render.com/register), then a new Blueprint
- Choose the GitHub project you pushed

<p className="text--center">
  <video src="/img/deploy/render-create-project.mp4" autoPlay={true} loop={true} style={{width: "100%", maxWidth:700}} controls={true} muted={true}/>
</p>

### Step 2: Add license key environment variable

- In the web server settings, set the `NEXT_PUBLIC_REPLICACHE_LICENSE_KEY` environment variable to your [Replicache License Key](/licensing).

<p className="text--center">
  <video src="/img/deploy/render-license-key.mp4" autoPlay={true} loop={true} style={{width: "100%", maxWidth:700}} controls={true} muted={true}/>
</p>

### Step 3: 🎉

When the deploy finishes, you should have a working app live on Render!

<p className="text--center">
  <video src="/img/deploy/render-success.mp4" autoPlay={true} loop={true} style={{width: "100%", maxWidth:700}} controls={true} muted={true}/>
</p>

# Next

You can also [deploy the quickstart app to Vercel/Supabase](/deploy-vercel-supabase).
