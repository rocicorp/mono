import type {Response} from 'express';
import type {Auth, DecodedIdToken} from 'firebase-admin/auth';
import type {Firestore} from 'firebase-admin/firestore';
import {https, logger} from 'firebase-functions';
import {defineString} from 'firebase-functions/params';
import {CallableRequest, onRequest} from 'firebase-functions/v2/https';
import assert from 'node:assert';
import {jsonSchema} from 'reflect-protocol';
import {Queue} from 'shared/src/queue.js';
import * as v from 'shared/src/valita.js';
import type WebSocket from 'ws';
import {createTail as createTailDefault} from '../../cloudflare/tail/tail.js';
import type express from 'express';
import packageJson from '../../../package.json';

import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {
  CreateTailRequest,
  createTailRequestSchema,
  createTailResponseSchema,
} from 'mirror-protocol/src/tail.js';
import type {IncomingHttpHeaders} from 'http';
import {decodeHeaderValue} from 'shared/src/headers.js';
import {defineSecretSafely} from '../app/secrets.js';

// This is the API token for reflect-server.net
// https://dash.cloudflare.com/085f6d8eb08e5b23debfb08b21bda1eb/
const cloudflareApiToken = defineSecretSafely('CLOUDFLARE_API_TOKEN');

const cloudflareAccountId = defineString('CLOUDFLARE_ACCOUNT_ID');

const validateFirebaseIdToken = async (
  auth: Auth,
  req: express.Request,
  res: express.Response,
): Promise<void | DecodedIdToken> => {
  console.log('Check if request is authorized with Firebase ID token');

  if (
    !req.headers.authorization ||
    !req.headers.authorization.startsWith('Bearer ')
  ) {
    console.error(
      'No Firebase ID token was passed as a Bearer token in the Authorization header.',
      'Make sure you authorize your request by providing the following HTTP header:',
      'Authorization: Bearer <Firebase ID Token>',
    );
    res.status(403).send('Unauthorized');
    return;
  }
  const idToken = req.headers.authorization.split('Bearer ')[1];
  const decodedIdToken = await auth
    .verifyIdToken(idToken)
    .then(decodedIdToken => decodedIdToken)
    .catch(error => {
      console.error('Error while verifying Firebase ID token:', error);
      res.status(401).send('Unauthorized');
    });
  return decodedIdToken;
};

export const create = (
  firestore: Firestore,
  auth: Auth,
  createTail = createTailDefault,
) => {
  const handler = validateSchema(
    createTailRequestSchema,
    createTailResponseSchema,
  )
    .validate(userAuthorization())
    .validate(appAuthorization(firestore))
    .handle(async (_tailRequest, context) => {
      const response = context.rawRequest.res;
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

      return {success: true};
    });

  return onRequest(
    async (request: express.Request, response: express.Response) => {
      const customRequest = request as https.Request;
      const authData = await validateFirebaseIdToken(auth, request, response);

      if (authData === undefined) {
        throw new Error('authData is undefined!');
      }
      const data = getData(request.headers);
      const callableRequest: CallableRequest<CreateTailRequest> = {
        auth: {
          uid: authData.uid,
          token: authData,
        },
        data,
        rawRequest: customRequest,
      };
      await handler(callableRequest);
    },
  );
};

function getData(headers: IncomingHttpHeaders): CreateTailRequest {
  const dataHeaderValue = headers['data'];
  if (!dataHeaderValue) {
    throw new Error('data header is missing');
  }
  if (typeof dataHeaderValue !== 'string') {
    throw new Error(
      'invalid data header, single value expected, received' +
        JSON.stringify(dataHeaderValue),
    );
  }
  const data = decodeHeaderValue(dataHeaderValue);
  return JSON.parse(data);
}

type QueueItem =
  | {type: 'data'; data: string}
  | {type: 'ping'}
  | {type: 'close'};

export function wsQueue(
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
