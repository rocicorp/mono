interface CustomHostMetadata {
  script: string;
}

interface Env {
  workers: DispatchNamespace;
}

export default {
  async fetch(req: Request<CustomHostMetadata>, env: Env) {
    try {
      // Namespaced (Workers for Platform) Workers are routed via Custom Hostname,
      // for which the metadata contains the name of the script to dispatch to.
      const name = (req.cf?.hostMetadata as CustomHostMetadata)?.script;
      if (name) {
        console.log(`Dispatching ${req.url} to ${name}`);
        const worker = env.workers.get(name);
        return await worker.fetch(req);
      }
      // Traditional Workers are routed via Custom Domain, for which Worker-to-Worker
      // dispatch works within the same zone.
      // https://developers.cloudflare.com/workers/configuration/routing/custom-domains/#interaction-with-routes
      console.log(
        `Dispatching ${req.url} via Custom Domain ${req.headers.get('host')}`,
      );
      return fetch(req);
    } catch (e) {
      if (!(e instanceof Error)) {
        return new Response(String(e), {status: 500});
      }
      if (e.message.startsWith('Worker not found')) {
        // We tried to get a worker that doesn't exist in our dispatch namespace
        return new Response('', {status: 404});
      }

      // This could be any other exception from `fetch()` *or* an exception
      // thrown by the called worker (e.g. if the dispatched worker has
      // `throw MyException()`, you could check for that here).
      return new Response(e.message, {status: 500});
    }
  },
};
