import * as functions from 'firebase-functions';
import cors from 'cors';
import {functionsConfig} from './functions-config.js';
import {publish as p} from './functions/publish.function.js';
import {healthcheck as h} from './functions/healthcheck.function.js';

// CORS configuration.
const options: cors.CorsOptions = {
  origin: functionsConfig.whitelist,
};

const withCors = fn => {
  return functions.https.onRequest((req, res) => {
    cors(options)(req, res, () => {
      fn(req, res);
    });
  });
};

export const publish = withCors(p);
export const healthcheck = withCors(h);
