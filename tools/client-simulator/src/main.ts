import {consoleLogSink, LogContext} from '@rocicorp/logger';
import 'dotenv/config';
import {nanoid} from 'nanoid';
import WebSocket from 'ws';
import {parseOptions} from '../../../packages/shared/src/options.ts';
import * as v from '../../../packages/shared/src/valita.ts';
import {initConnectionMessageSchema} from '../../../packages/zero-protocol/src/connect.ts';
import {downstreamSchema} from '../../../packages/zero-protocol/src/down.ts';
import {PROTOCOL_VERSION} from '../../../packages/zero-protocol/src/protocol-version.ts';
import initConnectionJSON from './init-connection.json' with {type: 'json'};

const options = {
  viewSyncers: {type: v.array(v.string())},

  numConnections: {type: v.number().default(1)},

  schemaVersion: {type: v.number().default(5)},
};

function run() {
  const lc = new LogContext('debug', {}, consoleLogSink);
  const {viewSyncers, numConnections, schemaVersion} = parseOptions(
    options,
    process.argv.slice(2),
    'ZERO_',
  );

  const initConnectionMessage = v.parse(
    initConnectionJSON,
    initConnectionMessageSchema,
  );

  let pokesReceived = 0;
  const clients: WebSocket[] = [];
  for (const vs of viewSyncers) {
    for (let i = 0; i < numConnections; i++) {
      const params = new URLSearchParams({
        clientGroupID: nanoid(10),
        clientID: nanoid(10),
        schemaVersion: String(schemaVersion),
        baseCookie: '',
        ts: String(performance.now()),
        lmid: '1',
      });
      const url = `${vs}/sync/v${PROTOCOL_VERSION}/connect?${params.toString()}`;
      const ws = new WebSocket(
        url,
        encodeURIComponent(btoa(JSON.stringify({initConnectionMessage}))),
      );
      lc.debug?.(`connecting to ${url}`);
      ws.on('error', err => lc.error?.(err));
      ws.on('open', () => lc.debug?.(`connected`));
      ws.addEventListener('message', ({data}) => {
        const message = v.parse(JSON.parse(data.toString()), downstreamSchema);
        switch (message[0]) {
          case 'error':
            lc.error?.(message);
            break;
          case 'pokeEnd':
            pokesReceived++;
            break;
        }
      });
      clients.push(ws);
    }
  }

  lc.info?.('');
  function logStatus() {
    process.stdout.write(`\rPOKES: ${pokesReceived}`);
  }
  const statusUpdater = setInterval(logStatus, 1000);

  process.on('SIGINT', () => {
    clients.forEach(ws => ws.close());
    clearInterval(statusUpdater);
  });
}

run();
