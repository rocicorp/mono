import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {nanoid} from 'nanoid';
import {pipeline, Writable} from 'node:stream';
import WebSocket, {createWebSocketStream} from 'ws';
import {parseOptions} from '../../../packages/shared/src/options.ts';
import * as v from '../../../packages/shared/src/valita.ts';
import {initConnectionMessageSchema} from '../../../packages/zero-protocol/src/connect.ts';
import {downstreamSchema} from '../../../packages/zero-protocol/src/down.ts';
import {PROTOCOL_VERSION} from '../../../packages/zero-protocol/src/protocol-version.ts';
import '../../shared/src/dotenv.ts';
import initConnectionJSON from './init-connection.json' with {type: 'json'};

const options = {
  viewSyncers: {type: v.array(v.string())},

  numConnections: {type: v.number().default(1)},

  millisBetweenMessages: {type: v.number().optional()},
};

function run() {
  const lc = new LogContext('debug', {}, consoleLogSink);
  const {viewSyncers, numConnections, millisBetweenMessages} = parseOptions(
    options,
    {envNamePrefix: 'ZERO_'},
  );

  const initConnectionMessage = v.parse(
    initConnectionJSON,
    initConnectionMessageSchema,
  );

  let pokesReceived = 0;
  let msgsReceived = 0;
  const clients: WebSocket[] = [];
  for (const vs of viewSyncers) {
    for (let i = 0; i < numConnections; i++) {
      const params = new URLSearchParams({
        clientGroupID: nanoid(10),
        clientID: nanoid(10),
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
      const stream = createWebSocketStream(ws);
      stream.on('error', err => lc.error?.(err));
      stream.on('open', () => lc.debug?.(`connected`));
      stream.on('close', () => lc.debug?.(`connection to ${url} closed`));
      pipeline(
        stream,
        new Writable({
          write: (data, _encoding, callback) => {
            try {
              const message = v.parse(
                JSON.parse(data.toString()),
                downstreamSchema,
              );
              const type = message[0];
              msgsReceived++;
              switch (type) {
                case 'error':
                  lc.error?.(message);
                  break;
                case 'pokeEnd':
                  pokesReceived++;
                  break;
              }
              if (millisBetweenMessages === undefined) {
                callback();
              } else {
                setTimeout(callback, millisBetweenMessages);
              }
            } catch (err) {
              callback(err instanceof Error ? err : new Error(String(err)));
            }
          },
        }),
        () => {},
      );
      clients.push(ws);
    }
  }

  lc.info?.('');
  function logStatus() {
    process.stdout.write(
      `\rPOKES:\t${pokesReceived}\tMESSAGES:\t${msgsReceived}`,
    );
  }
  const statusUpdater = setInterval(logStatus, 1000);

  process.on('SIGINT', () => {
    clients.forEach(ws => ws.close());
    clearInterval(statusUpdater);
  });
}

run();
