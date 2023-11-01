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
import {DEPLOYMENT_SECRETS_NAMES} from './functions/app/secrets.js';
import * as errorFunctions from './functions/error/index.js';
import * as roomFunctions from './functions/room/index.js';
import * as serverFunctions from './functions/server/index.js';
import * as teamFunctions from './functions/team/index.js';
import * as userFunctions from './functions/user/index.js';
import * as varsFunctions from './functions/vars/index.js';
import {SecretsClientImpl} from './secrets/index.js';

// Initializes firestore et al. (e.g. for subsequent calls to getFirestore())
initializeApp(appOptions);
setGlobalOptions({serviceAccount: serviceAccountId});

// Cache the secrets manager client to amortize connection establishment time.
// https://cloud.google.com/functions/docs/samples/functions-tips-gcp-apis#functions_tips_gcp_apis-nodejs
const secrets = new SecretsClientImpl();

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

export const error = {
  report: https.onCall(baseHttpsOptions, errorFunctions.report()),
};

export const app = {
  create: https.onCall(
    baseHttpsOptions,
    appFunctions.create(getFirestore(), secrets),
  ),
  publish: https.onCall(
    {
      ...baseHttpsOptions,
      secrets: [...DEPLOYMENT_SECRETS_NAMES],
      memory: '512MiB',
    },
    appFunctions.publish(
      getFirestore(),
      secrets,
      getStorage(),
      modulesBucketName,
    ),
  ),
  deploy: appFunctions.deploy(getFirestore(), getStorage(), secrets),
  autoDeploy: appFunctions.autoDeploy(getFirestore(), secrets),
  rename: https.onCall(baseHttpsOptions, appFunctions.rename(getFirestore())),
  tail: https.onRequest(
    {
      timeoutSeconds: 3600,
      ...baseHttpsOptions,
      secrets: [...DEPLOYMENT_SECRETS_NAMES],
    },
    appFunctions.tail(getFirestore(), getAuth(), secrets),
  ),
  delete: https.onCall(baseHttpsOptions, appFunctions.delete(getFirestore())),
};

export const room = {
  tail: https.onRequest(
    {
      timeoutSeconds: 3600,
      ...baseHttpsOptions,
      secrets: [...DEPLOYMENT_SECRETS_NAMES],
    },
    roomFunctions.tail(getFirestore(), getAuth(), secrets),
  ),
};

export const server = {
  autoDeploy: serverFunctions.autoDeploy(getFirestore(), secrets),
};

export const team = {
  ensure: https.onCall(baseHttpsOptions, teamFunctions.ensure(getFirestore())),
};

export const vars = {
  delete: https.onCall(baseHttpsOptions, varsFunctions.delete(getFirestore())),
  list: https.onCall(
    baseHttpsOptions,
    varsFunctions.list(getFirestore(), secrets),
  ),
  set: https.onCall(
    baseHttpsOptions,
    varsFunctions.set(getFirestore(), secrets),
  ),
};
