import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {sleep} from 'shared/src/sleep.js';
import {nanoid} from '../util/nanoid.js';
import {
  HTTPString,
  WSString,
  assertHTTPString,
  assertWSString,
  toWSString,
} from './http-string.js';

type CheckResult = {success: boolean; detail: string};
type Check = (l: LogContext) => Promise<CheckResult>;
type Checks = Record<string, Check>;

export async function checkConnectivity(
  reason: string,
  server: HTTPString,
  lc: LogContext,
) {
  const id = nanoid();
  lc = lc.withContext('connectCheckID', id).withContext('checkReason', reason);
  lc.info?.('Starting connectivity checks', {
    navigatorOnline: navigator.onLine,
  });
  const socketOrigin = toWSString(server);
  const checks: Checks = {
    cfGet: _ => checkCfGet(id, server),
    cfWebSocket: l => checkCfSocket(id, socketOrigin, false, l),
    cfWebSocketWSecWebSocketProtocolHeader: l =>
      checkCfSocket(id, socketOrigin, true, l),
    renderGet: _ => checkRenderGet(id),
    renderWebSocket: l => checkRenderSocket(id, false, l),
    renderWebSocketWSecWebSocketProtocolHeader: l =>
      checkRenderSocket(id, true, l),
  };

  const resultPs: Promise<CheckResult>[] = [];
  for (const [checkName, check] of Object.entries(checks)) {
    resultPs.push(
      (async () => {
        const checkLc = lc.withContext('checkName', checkName);
        checkLc.info?.('Starting check');
        let result: CheckResult;
        try {
          result = await check(checkLc);
        } catch (e) {
          const detail = `Error: ${
            e instanceof Error ? {name: e.name, message: e.message} : e
          }`;
          result = {success: false, detail};
        }
        checkLc.info?.(checkName, result);
        return result;
      })(),
    );
  }

  const results = await Promise.all(resultPs);
  lc.info?.(
    'Connectivity checks summary\n',
    ...Object.keys(checks).map(
      (checkName, i) => `${checkName}=${results[i].success}\n`,
    ),
    {
      navigatorOnline: navigator.onLine,
    },
  );
  lc.info?.(
    'Connectivity checks detail\n',
    ...Object.keys(checks).flatMap(
      (checkName, i) => `${checkName}=${results[i].detail}\n`,
    ),
    {
      navigatorOnline: navigator.onLine,
    },
  );
}

function checkRenderGet(id: string) {
  return checkGet(id, 'https://canary-render.onrender.com/canary/get');
}

function checkCfGet(id: string, server: HTTPString) {
  const cfGetCheckBaseURL = new URL(server);
  cfGetCheckBaseURL.pathname = '/api/canary/v0/get';
  const url = cfGetCheckBaseURL.toString();
  assertHTTPString(url);
  return checkGet(id, url);
}

function checkGet(id: string, baseURL: HTTPString) {
  const getCheckURL = new URL(baseURL);
  getCheckURL.searchParams.set('id', id);
  return Promise.race([
    timeout(),
    (async () => {
      const response = await fetch(getCheckURL);
      return {
        success: response.status === 200,
        detail: `Got response ${response.status} "${await response.text()}"`,
      };
    })(),
  ]);
}

function checkRenderSocket(
  id: string,
  wSecWebSocketProtocolHeader: boolean,
  lc: LogContext,
) {
  return checkSocket(
    id,
    'wss://canary-render.onrender.com/canary/websocket',
    wSecWebSocketProtocolHeader,
    lc,
  );
}

function checkCfSocket(
  id: string,
  socketOrigin: WSString,
  wSecWebSocketProtocolHeader: boolean,
  lc: LogContext,
) {
  const cfSocketCheckBaseURL = new URL(socketOrigin);
  cfSocketCheckBaseURL.pathname = '/api/canary/v0/websocket';
  const url = cfSocketCheckBaseURL.toString();
  assertWSString(url);
  return checkSocket(id, url, wSecWebSocketProtocolHeader, lc);
}

async function checkSocket(
  id: string,
  socketBaseURL: WSString,
  wSecWebSocketProtocolHeader: boolean,
  lc: LogContext,
) {
  const socketCheckURL = new URL(socketBaseURL);
  socketCheckURL.searchParams.set('id', id);
  socketCheckURL.searchParams.set(
    'wSecWebSocketProtocolHeader',
    wSecWebSocketProtocolHeader ? 'true' : 'false',
  );

  const cfWebSocket = wSecWebSocketProtocolHeader
    ? new WebSocket(socketCheckURL, 'check-' + id)
    : new WebSocket(socketCheckURL);

  const {promise, resolve} = resolver<CheckResult>();
  const onMessage = (e: MessageEvent<string>) => {
    lc.info?.('Received message', e.data);
    resolve({
      success: true,
      detail: `Connected and received message "${e.data}"`,
    });
  };
  const onOpen = () => {
    lc.info?.('Open event');
  };
  const onClose = (e: CloseEvent) => {
    const {code, reason, wasClean} = e;
    const closeInfo = {
      code,
      reason,
      wasClean,
    };
    lc.info?.('Received close', closeInfo);
    resolve({
      success: false,
      detail: `Closed before connected ${JSON.stringify(closeInfo)}.`,
    });
  };
  try {
    cfWebSocket.addEventListener('message', onMessage);
    cfWebSocket.addEventListener('open', onOpen);
    cfWebSocket.addEventListener('close', onClose);
    return await Promise.race([timeout(), promise]);
  } finally {
    cfWebSocket.removeEventListener('message', onMessage);
    cfWebSocket.removeEventListener('open', onOpen);
    cfWebSocket.removeEventListener('close', onClose);
    cfWebSocket.close();
  }
}

const TIMEOUT_MS = 10_000;

async function timeout(): Promise<CheckResult> {
  await sleep(TIMEOUT_MS);
  return {success: false, detail: 'Timed out.'};
}
