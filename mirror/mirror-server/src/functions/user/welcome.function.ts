import type {EventContext} from 'firebase-functions';
import type {UserRecord} from 'firebase-admin/auth';
import {defineSecretSafely} from '../app/secrets.js';
import {runWith} from 'firebase-functions';

const loopsApiKey = defineSecretSafely('LOOPS_API_KEY');

export const welcome = runWith({secrets: ['LOOPS_API_KEY']})
  .auth.user()
  .onCreate(async (user: UserRecord, _context: EventContext) => {
    const options = {
      method: 'POST',
      headers: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        'Authorization': `Bearer ${loopsApiKey.value()}`,
        'Content-Type': 'application/json',
      },
      body: `{"email":"${user.email}", "userId":"${user.uid}", "source": "prod"}`,
    };
    console.log('calling: https://app.loops.so/api/v1/contacts/create');
    await fetch('https://app.loops.so/api/v1/contacts/create', options)
      .then(response => response.json())
      .then(response => console.log(response))
      .catch(err => console.error(err));
  });
