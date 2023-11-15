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
import * as envFunctions from './functions/env/index.js';
import * as errorFunctions from './functions/error/index.js';
import * as metricsFunctions from './functions/metrics/index.js';
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
      memory: '512MiB',
    },
    appFunctions.publish(getFirestore(), getStorage(), modulesBucketName),
  ),
  deploy: appFunctions.deploy(getFirestore(), getStorage(), secrets),
  autoDeploy: appFunctions.autoDeploy(getFirestore()),
  rename: https.onCall(baseHttpsOptions, appFunctions.rename(getFirestore())),
  tail: https.onRequest(
    {
      ...baseHttpsOptions,
      timeoutSeconds: 3600,
    },
    appFunctions.tail(getFirestore(), getAuth(), secrets),
  ),
  delete: https.onCall(baseHttpsOptions, appFunctions.delete(getFirestore())),
};

export const env = {
  autoDeploy: envFunctions.autoDeploy(getFirestore()),
};

export const metrics = {
  aggregate: metricsFunctions.aggregate(getFirestore(), secrets),
  backup: metricsFunctions.backup(getFirestore(), getStorage(), secrets),
};

export const room = {
  tail: https.onRequest(
    {
      ...baseHttpsOptions,
      timeoutSeconds: 3600,
    },
    roomFunctions.tail(getFirestore(), getAuth(), secrets),
  ),
};

export const server = {
  autoDeploy: serverFunctions.autoDeploy(getFirestore()),
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
