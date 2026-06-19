import type {IncomingHttpHeaders} from 'node:http2';
import {must} from '../../../shared/src/must.ts';
import {
  decodeSecProtocols,
  type InitConnectionMessage,
} from '../../../zero-protocol/src/connect.ts';
import {URLParams} from '../types/url-params.ts';

export type ConnectParams = {
  readonly protocolVersion: number;
  readonly clientID: string;
  readonly clientGroupID: string;
  readonly profileID: string | null;
  readonly baseCookie: string | null;
  readonly timestamp: number;
  readonly lmID: number;
  readonly wsID: string;
  readonly debugPerf: boolean;
  readonly auth: string | undefined;
  readonly userID: string | undefined;
  readonly initConnectionMsg: InitConnectionMessage | undefined;
  readonly httpCookie: string | undefined;
  readonly origin: string | undefined;
  readonly requestHeaders?: Readonly<Record<string, string>> | undefined;
};

/**
 * Normalizes Node's {@link IncomingHttpHeaders} (whose values are
 * `string | string[] | undefined`) into a plain `Record<string, string>`,
 * joining array values with `, ` and dropping `undefined` values.
 */
function normalizeHeaders(
  headers: IncomingHttpHeaders,
): Record<string, string> {
  const normalized: Record<string, string> = Object.create(null);
  for (const [key, value] of Object.entries(headers)) {
    if (value === undefined) {
      continue;
    }
    normalized[key] = Array.isArray(value) ? value.join(', ') : value;
  }
  return normalized;
}

export function getConnectParams(
  protocolVersion: number,
  url: URL,
  headers: IncomingHttpHeaders,
):
  | {
      params: ConnectParams;
      error: null;
    }
  | {
      params: null;
      error: string;
    } {
  const params = new URLParams(url);

  try {
    const clientID = params.get('clientID', true);
    const clientGroupID = params.get('clientGroupID', true);
    const profileID = params.get('profileID', false);
    const baseCookie = params.get('baseCookie', false);
    const timestamp = params.getInteger('ts', true);
    const lmID = params.getInteger('lmid', true);
    const wsID = params.get('wsid', false) ?? '';
    const userID = params.get('userID', false) ?? undefined;
    const debugPerf = params.getBoolean('debugPerf');
    const {initConnectionMessage, authToken} = decodeSecProtocols(
      must(headers['sec-websocket-protocol']),
    );

    return {
      params: {
        protocolVersion,
        clientID,
        clientGroupID,
        profileID,
        baseCookie,
        timestamp,
        lmID,
        wsID,
        debugPerf,
        initConnectionMsg: initConnectionMessage,
        auth: authToken,
        userID,
        httpCookie: headers.cookie,
        origin: headers.origin,
        requestHeaders: normalizeHeaders(headers),
      },
      error: null,
    };
  } catch (e) {
    return {
      params: null,
      error: e instanceof Error ? e.message : String(e),
    };
  }
}
