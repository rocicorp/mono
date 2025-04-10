import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import {installWebSocketHandoff} from '../../services/dispatcher/websocket-handoff.ts';
import {HttpService, type Options} from '../../services/http-service.ts';
import type {IncomingMessageSubset} from '../../types/http.ts';
import type {Worker} from '../../types/processes.ts';

type Tenant = {
  // Note: The empty signifies the sole tenant. This can only be provided
  //       internally, as ID's specified via --tenants-json are normalized to
  //       be non-empty.
  id: string;
  host?: string | undefined;
  path?: string | undefined;
  worker: Worker;
};

export class ZeroDispatcher extends HttpService {
  readonly id = 'zero-dispatcher';
  readonly #tenants: Tenant[];
  readonly #runAsReplicationManager: boolean;

  constructor(
    lc: LogContext,
    runAsReplicationManager: boolean,
    tenants: Tenant[],
    opts: Options,
  ) {
    super('zero-dispatcher', lc, opts, fastify => {
      installWebSocketHandoff(lc, req => this.#handoff(req), fastify.server);
    });

    this.#runAsReplicationManager = runAsReplicationManager;
    this.#tenants = tenants.filter(
      // Only tenants with a host or path can be dispatched to
      // in the view-syncer (with the exception of the single tenant
      // case, signified by the empty id).
      t => runAsReplicationManager || t.host || t.path || t.id.length === 0,
    );
  }

  #handoff(req: IncomingMessageSubset) {
    const {headers, url: u} = req;
    const host = headers.host?.toLowerCase();
    const {pathname} = new URL(u ?? '', `http://${host}/`);

    for (const t of this.#tenants) {
      if (t.id.length === 0) {
        // sole tenant
        assert(this.#tenants.length === 1);
        return {payload: t.id, receiver: t.worker};
      }
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
      if (t.path && !pathname.startsWith(t.path)) {
        continue;
      }
      this._lc.debug?.(`connecting ${host}${pathname} to ${t.id}`);

      return {payload: t.id, receiver: t.worker};
    }
    throw new Error(`no matching tenant for: ${host}${pathname}`);
  }
}
