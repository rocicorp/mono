import {assert} from 'shared/src/asserts.js';

export function getConnectRequest(url: URL):
  | {
      result: {
        clientID: string;
        clientGroupID: string;
        baseCookie: string | null;
        timestamp: number;
        lmid: number;
        wsid: string;
        debugPerf: boolean;
      };
      error: null;
    }
  | {
      result: null;
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
    const lmid = getIntegerParam('lmid', true);
    const wsid = getParam('wsid', false) ?? '';
    const debugPerf = getBooleanParam('debugPerf');

    return {
      result: {
        clientID,
        clientGroupID,
        baseCookie,
        timestamp,
        lmid,
        wsid,
        debugPerf,
      },
      error: null,
    };
  } catch (e) {
    assert(e instanceof Error);

    return {
      result: null,
      error: e.message,
    };
  }
}
