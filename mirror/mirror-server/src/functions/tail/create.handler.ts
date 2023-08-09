import type {Auth} from 'firebase-admin/auth';
import type {Firestore} from 'firebase-admin/firestore';
import {onRequest} from 'firebase-functions/v2/https';

export const create = (_firestore: Firestore, _auth: Auth) =>
  onRequest(async (request, response) => {
    console.log(typeof request.body);
    // TODO(arv): Validate request.body
    // TODO(arv): userAuthorization()
    // TODO(arv): appAuthorization()

    response.writeHead(200, {
      'Cache-Control': 'no-store',
      'Content-Type': 'text/event-stream',
    });

    // const {tail, expiration, deleteTail} = await createTail(apiToken, accountID, cfWorkerName, filters, debug, env, packageVersion)

    console.log(`request.url: ${request.url}\n`);
    for (let i = 0; i < 10; i++) {
      response.write(`data: Item ${i}\n\n`);
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    response.end();
  });
