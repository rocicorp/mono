import {nanoid} from '../util/nanoid.js';
import {resolver} from '@rocicorp/resolver';
import {assert} from 'shared/asserts.js';
import {sleep} from 'shared/sleep.js';
import type {LogContext} from '@rocicorp/logger';

type Checks = Record<string, (l: LogContext) => Promise<string>>;

export async function checkConnectivity(
  reason: string,
  socketOrigin: string,
  lc: LogContext,
) {
  assert(socketOrigin.startsWith('ws://') || socketOrigin.startsWith('wss://'));
  const id = nanoid();
  lc = lc.withContext('connectCheckID', id).withContext('checkReason', reason);
  lc.info?.('Starting connectivity checks.');
  const checks: Checks = {
    cfGet: _ => checkCfGet(id, socketOrigin),
    cfWebSocket: l => checkCfSocket(id, socketOrigin, false, l),
    cfWebSocketWSecWebSocketProtocolHeader: l =>
      checkCfSocket(id, socketOrigin, true, l),
  };

  const resultPs: Promise<unknown[]>[] = [];
  for (const [checkName, check] of Object.entries(checks)) {
    resultPs.push(
      (async () => {
        const checkLc = lc.withContext('checkName', checkName);
        checkLc.info?.('Starting check');
        let result;
        try {
          result = [`${checkName} result:`, await check(checkLc)];
        } catch (e) {
          const eDetails =
            e instanceof Error ? {name: e.name, message: e.message} : e;
          result = [`${checkName} error:`, eDetails];
        }
        checkLc.info?.(...result);
        return result;
      })(),
    );
  }

  const results = await Promise.all(resultPs);
  lc.info?.(
    'Connectivity check results\n',
    ...results.flatMap(r => [...r, '\n']),
  );
}

function checkCfGet(id: string, socketOrigin: string) {
  const cfGetCheckUrl = new URL(socketOrigin.replace(/^ws/, 'http'));
  cfGetCheckUrl.pathname = '/api/debug/v0/get';
  cfGetCheckUrl.searchParams.set('id', id);
  return Promise.race([
    timeout(),
    (async () => {
      const response = await fetch(cfGetCheckUrl);
      return `Got response ${response.status} ${await response.text()}.`;
    })(),
  ]);
}

async function checkCfSocket(
  id: string,
  socketOrigin: string,
  wSecWebSocketProtocolHeader: boolean,
  lc: LogContext,
) {
  const cfSocketCheckUrl = new URL(socketOrigin);
  cfSocketCheckUrl.pathname = '/api/debug/v0/websocket';
  cfSocketCheckUrl.searchParams.set('id', id);
  cfSocketCheckUrl.searchParams.set(
    'wSecWebSocketProtocolHeader',
    wSecWebSocketProtocolHeader ? 'true' : 'false',
  );

  const cfWebSocket = wSecWebSocketProtocolHeader
    ? new WebSocket(cfSocketCheckUrl, 'check-' + id)
    : new WebSocket(cfSocketCheckUrl);

  const {promise, resolve} = resolver<string>();
  const onMessage = (e: MessageEvent<string>) => {
    lc.info?.('Received message', e.data);
    resolve(`Connected and received message "${e.data}"`);
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
    resolve(`Closed before connected ${JSON.stringify(closeInfo)}`);
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

async function timeout(): Promise<string> {
  await sleep(TIMEOUT_MS);
  return 'Timed out.';
}
