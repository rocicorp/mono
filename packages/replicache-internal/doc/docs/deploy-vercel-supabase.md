---
title: Deploy to Vercel and Supabase
slug: /deploy-vercel-supabase
---

You can host Replicache apps on serverless platforms like [Vercel](https://vercel.com).

We recommend pairing with [Supabase](https://supabase.com) for storage, since Supabase also has realtime features that can be used for Replicache's [poke message](/how-it-works#poke-optional).

### Step 1: Create Supabase Project

- Create a new project at [Supabase](https://supabase.com).
- Create in `East US` because that's where Vercel functions are deployed by default, and they should be close.
- **Important:** Don't forget to copy the password you choose to some scratch file. You'll need that later and can't retrieve it after this screen.

<p className="text--center">
  <video src="/img/deploy/vercel-create-supabase-project.mp4" autoPlay={true} loop={true} style={{width: "100%", maxWidth:700}} controls={true} muted={true}/>
</p>

### Step 2: Create Vercel Project

- Push your Replicache app to GitHub.
- Create a new project at [Vercel](https://vercel.com) and link to the GitHub project.
- **Note:** This will deploy but the app will not run yet. We will fix that next.

<p className="text--center">
  <video src="/img/deploy/vercel-create-project.mp4" autoPlay={true} loop={true} style={{width: "100%", maxWidth:700}} controls={true} muted={true}/>
</p>

### Step 3: Add Supabase Integration to Vercel Project

- From your Vercel project's dashboard, go to Settings > Integrations.
- Add the Supabase integration and connect to your Supabase project.
- This will add several environment variables to your project (you may have to refresh the page before they appear).
- **Note:** It still won't quite work. One more step to go.

<p className="text--center">
  <video src="/img/deploy/vercel-integrate-supabase.mp4" autoPlay={true} loop={true} style={{width: "100%", maxWidth:700}} controls={true} muted={true}/>
</p>

### Step 4: Add Final Environment Variables

- From your Vercel project's dashboard, go to Settings > Environment Variables.
- Add `NEXT_PUBLIC_REPLICACHE_LICENSE_KEY` with your [Replicache License Key](/licensing).
- Add `SUPABASE_DATABASE_PASSWORD` with the password you copied from your Supabase setup in Step 1.
- Go to Deployments and redeploy the latest build to pick up the new variables.

<p className="text--center">
  <video src="/img/deploy/vercel-final-env-vars.mp4" autoPlay={true} loop={true} style={{width: "100%", maxWidth:700}} controls={true} muted={true}/>
</p>

### Step 5: 🎉

When the deploy finishes, you should have a working app live on Vercel and Supabase!

<p className="text--center">
  <video src="/img/deploy/vercel-success.mp4" autoPlay={true} loop={true} style={{width: "100%", maxWidth:700}} controls={true} muted={true}/>
</p>
