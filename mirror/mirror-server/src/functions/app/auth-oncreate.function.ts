import type {EventContext} from 'firebase-functions';
import type {UserRecord} from 'firebase-admin/auth';
import {defineSecretSafely} from './secrets.js';
import {runWith} from 'firebase-functions';

const loopsApiKey = defineSecretSafely('LOOPS_API_KEY');

export const authOnCreate = runWith({secrets: ['LOOPS_API_KEY']})
  .auth.user()
  .onCreate(async (user: UserRecord, _context: EventContext) => {
    const options = {
      method: 'POST',
      // eslint-disable-next-line @typescript-eslint/naming-convention
      headers: {Authorization: `Bearer ${loopsApiKey}`},
      body: `{"email":${user.email}, "userId":${user.uid}}, "source": 'prod'`,
    };

    await fetch('https://app.loops.so/api/v1/contacts/create', options).then(
      response => response.json(),
    );
  });
