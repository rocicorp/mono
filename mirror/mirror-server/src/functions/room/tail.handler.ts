import type {Response} from 'express';
import type {Auth} from 'firebase-admin/auth';
import type {Firestore} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {HttpsError, onRequest} from 'firebase-functions/v2/https';
import {tailMessageSchema} from 'mirror-protocol/src/tail-message.js';
import {roomTailRequestSchema} from 'mirror-protocol/src/tail.js';
import type {App} from 'mirror-schema/src/app.js';
import assert from 'node:assert';
import {must} from 'shared/src/must.js';
import {Queue} from 'shared/src/queue.js';
import * as valita from 'shared/src/valita.js';
import WebSocket from 'ws';
import {SecretsCache, SecretsClient} from '../../secrets/index.js';
import {REFLECT_AUTH_API_KEY, getAppSecrets} from '../app/secrets.js';
import {
  appAuthorization,
  tokenAuthentication,
  userAuthorization,
} from '../validators/auth.js';
import {validateRequest} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';

export const tail = (
  firestore: Firestore,
  auth: Auth,
  secretsClient: SecretsClient,
  createTail = createTailDefault,
) =>
  onRequest(
    validateRequest(roomTailRequestSchema)
      .validate(tokenAuthentication(auth))
      .validate(userAgentVersion())
      .validate(userAuthorization())
      .validate(appAuthorization(firestore))
      .handle(async (tailRequest, context) => {
        const secrets = new SecretsCache(secretsClient);
        const {response, app} = context;
        const {
          appID,
          roomID,
          requester: {userAgent},
        } = tailRequest;

        const {name, runningDeployment} = app;
        if (!runningDeployment) {
          throw new HttpsError(
            'failed-precondition',
            `App ${name} is not running. Please run 'npm @rocicorp/reflect publish'`,
          );
        }

        const {secrets: appSecrets} = await getAppSecrets(secrets, app.secrets);

        const reflectAuthApiKey = appSecrets[REFLECT_AUTH_API_KEY];
        if (!reflectAuthApiKey) {
          throw new HttpsError(
            'internal',
            `App ${appID} is missing an API key`,
          );
        }

        let ws: WebSocket;
        try {
          ws = createTail(
            app,
            roomID,
            reflectAuthApiKey,
            `${userAgent.type}/${userAgent.version}`,
          );
        } catch (e) {
          throw new HttpsError('internal', `Failed to connect to backend`, e);
        }

        response.writeHead(200, {
          'Cache-Control': 'no-store',
          'Content-Type': 'text/event-stream',
        });
        response.flushHeaders();

        try {
          loop: for await (const item of wsQueue(ws, 10_000)) {
            switch (item.type) {
              case 'data':
                logger.debug('Got data from tail websocket', item.data);
                writeData(response, item.data);
                break;
              case 'ping':
                logger.debug('Got ping from tail websocket');
                response.write('\n\n');
                break;
              case 'close':
                logger.debug('Got close from tail websocket');
                break loop;
            }
          }
        } catch (e) {
          logger.info(
            'Got exception from tail websocket',
            e,
            'forwarding error to SSE',
          );
          writeEvent(response, 'error', hasStringMessage(e) ? e.message : '');
        } finally {
          ws.close();
        }
        response.end();
      }),
  );

function hasStringMessage(v: unknown): v is {message: string} {
  return (
    typeof v === 'object' &&
    v !== null &&
    'message' in v &&
    typeof v.message === 'string'
  );
}

type QueueItem =
  | {type: 'data'; data: string}
  | {type: 'ping'}
  | {type: 'close'};

function dataAsString(e: WebSocket.MessageEvent): string {
  const {data} = e;
  if (typeof data === 'string') {
    return data;
  }
  assert(data instanceof Buffer);
  return data.toString('utf-8');
}

function wsQueue(
  ws: WebSocket,
  pingInterval: number,
): AsyncIterable<QueueItem> {
  const q = new Queue<QueueItem>();
  ws.onmessage = e => {
    void q.enqueue({type: 'data', data: dataAsString(e)});
  };

  ws.onerror = e => void q.enqueueRejection(e);
  ws.onclose = () => {
    void q.enqueue({type: 'close'});
  };

  const pingTimer = setInterval(
    () => void q.enqueue({type: 'ping'}),
    pingInterval,
  );

  function cleanup() {
    clearInterval(pingTimer);
    ws.close();
  }

  return {
    [Symbol.asyncIterator]: () => q.asAsyncIterator(cleanup),
  };
}

function writeData(response: Response, data: string) {
  const parsedData = valita.parse(JSON.parse(data), tailMessageSchema, 'strip');
  response.write(`data: ${JSON.stringify(parsedData)}\n\n`);
}

function writeEvent(response: Response, event: string, data: string) {
  response.write(`event: ${event}\n`);
  response.write(`data: ${data}\n\n`);
}

function createTailDefault(
  app: App,
  roomID: string,
  reflectAPIToken: string,
  packageVersion: string,
): WebSocket {
  const {hostname} = must(app.runningDeployment).spec;

  const websocketUrl = `wss://${hostname}/api/debug/v0/tail?roomID=${encodeURIComponent(
    roomID,
  )}`;
  // For tail we send the REFLECT_AUTH_API_KEY in the Sec-WebSocket-Protocol
  // header and it is always required
  return new WebSocket(websocketUrl, reflectAPIToken, {
    headers: {
      'User-Agent': `reflect/${packageVersion}`,
    },
  });
}
