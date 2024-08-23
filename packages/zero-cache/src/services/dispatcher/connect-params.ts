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
  function getParam(name: string, required: true): string;
  function getParam(name: string, required: boolean): string | null;
  function getParam(name: string, required: boolean) {
    const value = url.searchParams.get(name);
    if (value === '' || value === null) {
      if (required) {
        throw new Error(`invalid querystring - missing ${name}`);
      }
      return null;
    }
    return value;
  }

  function getIntegerParam(name: string, required: true): number;
  function getIntegerParam(name: string, required: boolean): number | null;
  function getIntegerParam(name: string, required: boolean) {
    const value = getParam(name, required);
    if (value === null) {
      return null;
    }
    const int = parseInt(value);
    if (isNaN(int)) {
      throw new Error(
        `invalid querystring parameter ${name}, got: ${value}, url: ${url}`,
      );
    }
    return int;
  }

  function getBooleanParam(name: string): boolean {
    const value = getParam(name, false);
    if (value === null) {
      return false;
    }
    return value === 'true';
  }

  try {
    const clientID = getParam('clientID', true);
    const clientGroupID = getParam('clientGroupID', true);
    const baseCookie = getParam('baseCookie', false);
    const timestamp = getIntegerParam('ts', true);
    const lmID = getIntegerParam('lmid', true);
    const wsID = getParam('wsid', false) ?? '';
    const debugPerf = getBooleanParam('debugPerf');

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
