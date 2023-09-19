interface Namespace {
  get(name: string): Fetcher;
}

interface CustomHostMetadata {
  script_name: string;
}

interface Env {
  workers: Namespace;
}

export default {
  async fetch(req: Request<CustomHostMetadata>, env: Env) {
    try {
      const name = (req.cf?.hostMetadata as CustomHostMetadata)?.script_name;
      if (!name) {
        return new Response(`No metadata for ${req.headers.get('host')}`, {
          status: 400,
        });
      }
      console.log(`Dispatching ${req.url} to ${name}`);
      const worker = env.workers.get(name);
      return await worker.fetch(req);
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
