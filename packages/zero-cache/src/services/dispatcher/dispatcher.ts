import {LogContext} from '@rocicorp/logger';
import {IncomingMessage} from 'http';
import UrlPattern from 'url-pattern';
import {h32} from '../../../../shared/src/xxhash.js';
import type {Worker} from '../../types/processes.js';
import {HttpService, type Options} from '../http-service.js';
import {getConnectParams} from './connect-params.js';
import {installWebSocketHandoff} from './websocket-handoff.js';

const CONNECT_URL_PATTERNS = [
  new UrlPattern('/zero/sync/:version/connect'),
  new UrlPattern('/api/sync/:version/connect'),
];

const SUPPORTED_VERSION = 'v1';

export type Workers = {
  syncers: Worker[];
};

export class Dispatcher extends HttpService {
  readonly id = 'dispatcher';
  readonly #workersByHostname: (hostname: string) => Workers;

  constructor(
    lc: LogContext,
    workersByHostname: (hostname: string) => Workers,
    opts: Options,
  ) {
    super('dispatcher', lc, opts, fastify => {
      fastify.get('/', (_req, res) => res.send('OK'));
      installWebSocketHandoff(fastify.server, req => this.#handoff(req));
    });

    this.#workersByHostname = workersByHostname;
  }

  #handoff(req: IncomingMessage) {
    const {headers, url: u} = req;
    const url = new URL(u ?? '', 'http://unused/');
    const syncPath = parseSyncPath(url);
    if (!syncPath) {
      throw new Error(`Invalid sync URL: ${u}`);
    }
    if (syncPath.version !== SUPPORTED_VERSION) {
      throw new Error(`Unsupported sync version: ${u}`);
    }
    const {params, error} = getConnectParams(url, headers);
    if (error !== null) {
      throw new Error(error);
    }
    const {host} = headers;
    if (!host) {
      throw new Error('Missing Host field');
    }
    const {clientGroupID} = params;
    const {syncers} = this.#workersByHostname(host);
    const syncer = h32(clientGroupID) % syncers.length;

    this._lc.debug?.(`connecting ${clientGroupID} to syncer ${syncer}`);
    return {payload: params, receiver: syncers[syncer]};
  }
}

function parseSyncPath(url: URL): {version: string} | undefined {
  for (const pattern of CONNECT_URL_PATTERNS) {
    const m = pattern.match(url.pathname);
    if (m) {
      return m;
    }
  }
  return undefined;
}
