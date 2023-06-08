import {https} from 'firebase-functions/v2';
import {initializeApp} from 'firebase-admin/app';
import {functionsConfig} from './functions-config.js';
import {publish as publishHandler} from './functions/publish.function.js';
import {healthcheck as healthcheckHandler} from './functions/healthcheck.function.js';
import * as userFunctions from './functions/user/index.js';

// Initializes firestore and auth clients.
initializeApp();

export const publish = https.onRequest(
  {cors: functionsConfig.allowlist},
  publishHandler,
);
export const healthcheck = https.onRequest(
  {cors: functionsConfig.allowlist},
  healthcheckHandler,
);

export const user = {
  ensure: https.onCall({cors: functionsConfig.allowlist}, userFunctions.ensure),
};
