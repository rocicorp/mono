import type {LogContext} from '@rocicorp/logger';
import {installWebSocketHandoff} from '../../services/dispatcher/websocket-handoff.ts';
import {HttpService, type Options} from '../../services/http-service.ts';
import type {IncomingMessageSubset} from '../../types/http.ts';
import type {Worker} from '../../types/processes.ts';

type Tenant = {
  id: string;
  host?: string | undefined;
  path?: string | undefined;
  worker: Worker;
};

export class TenantDispatcher extends HttpService {
  readonly id = 'tenant-dispatcher';
  readonly #tenants: Tenant[];
  readonly #runAsReplicationManager: boolean;

  constructor(
    lc: LogContext,
    runAsReplicationManager: boolean,
    tenants: Tenant[],
    opts: Options,
  ) {
    super('tenant-dispatcher', lc, opts, fastify => {
      installWebSocketHandoff(lc, req => this.#handoff(req), fastify.server);
    });

    this.#runAsReplicationManager = runAsReplicationManager;
    this.#tenants = tenants.filter(
      // Only tenants with a host or path can be dispatched to
      // in the view-syncer.
      t => runAsReplicationManager || t.host || t.path,
    );
  }

  #handoff(req: IncomingMessageSubset) {
    const {headers, url: u} = req;
    const host = headers.host?.toLowerCase();
    const {pathname} = new URL(u ?? '', `http://${host}/`);

    for (const t of this.#tenants) {
      if (this.#runAsReplicationManager) {
        // The replication-manager dispatches internally using the
        // tenant ID as the first path component
        if (pathname.startsWith('/' + t.id + '/')) {
          return {payload: t.id, receiver: t.worker};
        }
        continue;
      }
      if (t.host && t.host !== host) {
        continue;
      }
      if (t.path && pathname !== t.path && !pathname.startsWith(t.path + '/')) {
        continue;
      }
      this._lc.debug?.(`connecting ${host}${pathname} to ${t.id}`);

      return {payload: t.id, receiver: t.worker};
    }
    throw new Error(`no matching tenant for: ${host}${pathname}`);
  }
}
