import {URLParams} from 'zero-cache/src/types/url-params.js';

export type ConnectParams = {
  readonly clientID: string;
  readonly clientGroupID: string;
  readonly baseCookie: string | null;
  readonly timestamp: number;
  readonly lmID: number;
  readonly wsID: string;
  readonly debugPerf: boolean;
};

export function getConnectParams(url: URL):
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
    const baseCookie = params.get('baseCookie', false);
    const timestamp = params.getInteger('ts', true);
    const lmID = params.getInteger('lmid', true);
    const wsID = params.get('wsid', false) ?? '';
    const debugPerf = params.getBoolean('debugPerf');

    return {
      params: {
        clientID,
        clientGroupID,
        baseCookie,
        timestamp,
        lmID,
        wsID,
        debugPerf,
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
