import {initializeApp} from 'firebase-admin/app';
import {getAuth} from 'firebase-admin/auth';
import {getFirestore} from 'firebase-admin/firestore';
import {getStorage} from 'firebase-admin/storage';
import {https, setGlobalOptions} from 'firebase-functions/v2';
import {
  appOptions,
  baseHttpsOptions,
  modulesBucketName,
  serviceAccountId,
} from './config/index.js';
import * as appFunctions from './functions/app/index.js';
import {healthcheck as healthcheckHandler} from './functions/healthcheck.function.js';
import * as serverFunctions from './functions/server/index.js';
import * as teamFunctions from './functions/team/index.js';
import * as userFunctions from './functions/user/index.js';
import {DEPLOYMENT_SECRETS_NAMES} from './functions/app/secrets.js';

// Initializes firestore et al. (e.g. for subsequent calls to getFirestore())
initializeApp(appOptions);
setGlobalOptions({serviceAccount: serviceAccountId});

export const healthcheck = https.onRequest(
  baseHttpsOptions,
  healthcheckHandler,
);

// Per https://firebase.google.com/docs/functions/manage-functions
// functions should be deployed in groups of 10 or fewer
// Best practice is to organize the functions by grouping them
// into separate logical groups and exporting each group here.
// Then deployment should take place on a group by group basis
// or deploy individual updated functions
export const user = {
  ensure: https.onCall(
    baseHttpsOptions,
    userFunctions.ensure(getFirestore(), getAuth()),
  ),
};

export const app = {
  create: https.onCall(baseHttpsOptions, appFunctions.create(getFirestore())),
  publish: https.onCall(
    {...baseHttpsOptions, secrets: [...DEPLOYMENT_SECRETS_NAMES]},
    appFunctions.publish(getFirestore(), getStorage(), modulesBucketName),
  ),
  deploy: appFunctions.deploy(getFirestore(), getStorage()),
  autoDeploy: appFunctions.autoDeploy(getFirestore()),
  rename: https.onCall(baseHttpsOptions, appFunctions.rename(getFirestore())),
  tail: https.onRequest(
    {
      timeoutSeconds: 3600,
      ...baseHttpsOptions,
      secrets: ['CLOUDFLARE_API_TOKEN', ...DEPLOYMENT_SECRETS_NAMES],
    },
    appFunctions.tail(getFirestore(), getAuth()),
  ),
};

export const server = {
  autoDeploy: serverFunctions.autoDeploy(getFirestore()),
};

export const team = {
  ensure: https.onCall(baseHttpsOptions, teamFunctions.ensure(getFirestore())),
};
