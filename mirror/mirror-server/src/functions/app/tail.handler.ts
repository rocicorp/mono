import type {Response} from 'express';
import type {Auth} from 'firebase-admin/auth';
import type {Firestore} from 'firebase-admin/firestore';
import {https, logger} from 'firebase-functions';
import {defineString} from 'firebase-functions/params';
import {onRequest} from 'firebase-functions/v2/https';
import assert from 'node:assert';
import {jsonSchema} from 'reflect-protocol';
import {Queue} from 'shared/src/queue.js';
import * as v from 'shared/src/valita.js';
import type WebSocket from 'ws';
import {createTail as createTailDefault} from '../../cloudflare/tail/tail.js';
import packageJson from '../../../package.json';

import {
  appAuthorization,
  tokenAuthentication,
  userAuthorization,
} from '../validators/auth.js';
import {validateRequest} from '../validators/schema.js';
import {tailRequestSchema} from 'mirror-protocol/src/tail.js';
import {defineSecretSafely} from './secrets.js';

// This is the API token for reflect-server.net
// https://dash.cloudflare.com/085f6d8eb08e5b23debfb08b21bda1eb/
const cloudflareApiToken = defineSecretSafely('CLOUDFLARE_API_TOKEN');

const cloudflareAccountId = defineString('CLOUDFLARE_ACCOUNT_ID');

export const tail = (
  firestore: Firestore,
  auth: Auth,
  createTail = createTailDefault,
) =>
  onRequest(
    validateRequest(tailRequestSchema)
      .validate(tokenAuthentication(auth))
      .validate(userAuthorization())
      .validate(appAuthorization(firestore))
      .handle(async (_tailRequest, context) => {
        const {response} = context;
        if (response === undefined) {
          throw new https.HttpsError('not-found', 'response is undefined');
        }
        response.writeHead(200, {
          'Cache-Control': 'no-store',
          'Content-Type': 'text/event-stream',
        });
        response.flushHeaders();

        const apiToken = cloudflareApiToken.value();
        const accountID = cloudflareAccountId.value();
        const cfWorkerName = context.app.cfScriptName;
        const filters = {filters: []};
        const debug = true;
        const env = undefined;
        const packageVersion = packageJson.version || '0.0.0';

        const {ws, expiration, deleteTail} = await createTail(
          apiToken,
          accountID,
          cfWorkerName,
          filters,
          debug,
          env,
          packageVersion,
        );

        logger.log(`expiration: ${expiration}`);

        try {
          loop: for await (const item of wsQueue(ws, 10_000)) {
            switch (item.type) {
              case 'data':
                writeData(response, item.data);
                break;
              case 'ping':
                response.write('\n\n');
                break;
              case 'close':
                break loop;
            }
          }
        } catch (e) {
          logger.error(e);
        } finally {
          await deleteTail();
        }
        response.end();
      }),
  );

type QueueItem =
  | {type: 'data'; data: string}
  | {type: 'ping'}
  | {type: 'close'};

function wsQueue(
  ws: WebSocket,
  pingInterval: number,
): AsyncIterable<QueueItem> {
  const q = new Queue<QueueItem>();
  ws.onmessage = ({data}) => {
    assert(data instanceof Buffer);
    void q.enqueue({type: 'data', data: data.toString('utf-8')});
  };

  ws.onerror = event => void q.enqueueRejection(event);
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

const partialRecordSchema = v.object({
  logs: v.array(
    v.object({
      message: jsonSchema,
      level: v.string(),
      timestamp: v.number(),
    }),
  ),
});

export function writeData(response: Response, data: string) {
  const cfLogRecord = JSON.parse(data);
  const logRecords = v.parse(cfLogRecord, partialRecordSchema, 'strip');
  for (const rec of logRecords.logs) {
    response.write(`data: ${JSON.stringify(rec)}\n\n`);
  }
}
