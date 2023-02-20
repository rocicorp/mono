import '@cloudflare/workers-types';

interface Bindings {
  roomDO: DurableObjectNamespace;
  authDO: DurableObjectNamespace;
}
