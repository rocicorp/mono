import type {LogContext} from '@rocicorp/logger';
import {installWebSocketHandoff} from '../../services/dispatcher/websocket-handoff.js';
import {HttpService, type Options} from '../../services/http-service.js';
import type {IncomingMessageSubset} from '../../types/http.js';
import type {Worker} from '../../types/processes.js';

type Tenant = {
  id: string;
  host?: string | undefined;
  path?: string | undefined;
  worker: Worker;
};

export class TenantDispatcher extends HttpService {
  readonly id = 'tenant-dispatcher';
  readonly #tenants: Tenant[];

  constructor(lc: LogContext, tenants: Tenant[], opts: Options) {
    super('tenant-dispatcher', lc, opts, fastify => {
      fastify.get('/', (_req, res) => res.send('OK'));
      installWebSocketHandoff(lc, req => this.#handoff(req), fastify.server);
    });

    // Only tenants with a host or path can be dispatched to.
    this.#tenants = tenants.filter(t => t.host || t.path);
  }

  #handoff(req: IncomingMessageSubset) {
    const {headers, url: u} = req;
    const host = headers.host?.toLowerCase();
    const {pathname} = new URL(u ?? '', `http://${host}/`);

    for (const t of this.#tenants) {
      if (t.host && t.host !== host) {
        continue;
      }
      if (t.path && pathname !== t.path && !pathname.startsWith(t.path + '/')) {
        continue;
      }
      this._lc.debug?.(`connecting ${host}${pathname} to ${t.id}`);

      return {payload: t.id, receiver: t.worker};
    }
    throw new Error(`no matching tenant for: ${u}`);
  }
}
