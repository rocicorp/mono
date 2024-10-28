import {LogContext} from '@rocicorp/logger';
import {IncomingMessage} from 'http';
import {h32} from '../../../../shared/src/xxhash.js';
import type {Worker} from '../../types/processes.js';
import {HttpService, type Options} from '../http-service.js';
import {getConnectParams} from './connect-params.js';
import {installWebSocketHandoff} from './websocket-handoff.js';

export const CONNECT_URL_PATTERN = '/api/sync/:version/connect';

export type Workers = {
  syncers: Worker[];
};

export const DEFAULT_PORT = 4848;

export class Dispatcher extends HttpService {
  readonly id = 'dispatcher';
  readonly #workersByHostname: (hostname: string) => Workers;

  constructor(
    lc: LogContext,
    workersByHostname: (hostname: string) => Workers,
    opts: Options = {port: DEFAULT_PORT},
  ) {
    super('dispatcher', lc, opts, fastify => {
      fastify.get('/', (_req, res) => res.send('OK'));
      installWebSocketHandoff(fastify.server, req => this.#handoff(req));
    });

    this.#workersByHostname = workersByHostname;
  }

  #handoff(req: IncomingMessage) {
    const {headers, url} = req;
    const {params, error} = getConnectParams(
      new URL(url ?? '', 'http://unused/'),
      headers,
    );
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
