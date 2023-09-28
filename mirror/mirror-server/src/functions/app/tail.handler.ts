import type {Response} from 'express';
import type {Auth} from 'firebase-admin/auth';
import type {Firestore} from 'firebase-admin/firestore';
import {https, logger} from 'firebase-functions';
import {HttpsError, onRequest} from 'firebase-functions/v2/https';
import {tailRequestSchema} from 'mirror-protocol/src/tail.js';
import assert from 'node:assert';
import {jsonSchema} from 'reflect-protocol';
import {Queue} from 'shared/src/queue.js';
import * as v from 'shared/src/valita.js';
import type WebSocket from 'ws';
import packageJson from '../../../package.json';
import {createTail as createTailDefault} from '../../cloudflare/tail/tail.js';
import {
  appAuthorization,
  tokenAuthentication,
  userAuthorization,
} from '../validators/auth.js';
import {validateRequest} from '../validators/schema.js';
import {getApiToken} from './secrets.js';
import {GlobalScript} from 'cloudflare-api/src/scripts.js';
import {
  providerDataConverter,
  providerPath,
} from 'mirror-schema/src/provider.js';
import {getDataOrFail} from '../validators/data.js';

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
      .handle(async (tailRequest, context) => {
        const {response} = context;
        if (response === undefined) {
          throw new https.HttpsError('not-found', 'response is undefined');
        }

        const {appID} = tailRequest;
        const {cfScriptName: cfWorkerName, scriptRef, provider} = context.app;

        if (scriptRef) {
          throw new HttpsError(
            'unavailable',
            'The App does not support this version of tail. ' +
              'Please try again with `npx @rocicorp/reflect@latest`.',
          );
        }

        const apiToken = getApiToken(provider);
        const {accountID} = getDataOrFail(
          await firestore
            .doc(providerPath(provider))
            .withConverter(providerDataConverter)
            .get(),
          'internal',
          `Unknown provider "${provider}" for App ${appID} `,
        );

        const filters = {filters: []};
        const debug = true;
        const packageVersion = packageJson.version || '0.0.0';

        let createTailResult;
        try {
          createTailResult = await createTail(
            new GlobalScript(
              {apiToken: await apiToken, accountID},
              cfWorkerName,
            ),
            filters,
            debug,
            packageVersion,
          );
        } catch (e) {
          throw new HttpsError('internal', `Failed to connect to backend`, e);
        }

        response.writeHead(200, {
          'Cache-Control': 'no-store',
          'Content-Type': 'text/event-stream',
        });
        response.flushHeaders();

        const {ws, expiration, deleteTail} = createTailResult;
        // TODO(arv): Do we need to deal with the expiration?
        logger.log(`tail expiration: ${expiration}`);

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
