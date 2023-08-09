import type {Auth} from 'firebase-admin/auth';
import type {Firestore} from 'firebase-admin/firestore';
import {onRequest} from 'firebase-functions/v2/https';

export const create = (_firestore: Firestore, _auth: Auth) =>
  onRequest(async (request, response) => {
    response.writeHead(200, {
      'Cache-Control': 'no-store',
      'Content-Type': 'text/event-stream',
      // 'Transfer-Encoding': 'chunked',
    });
    response.flushHeaders();
    console.log(`request.url: ${request.url}\n`);
    for (let i = 0; i < 100; i++) {
      await new Promise<void>((resolve, reject) => {
        console.log(`Writing Item ${i}`);
        response.write(`data: Item ${i}\n\n`, err => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      });
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    response.end();
  });
