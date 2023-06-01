import * as functions from 'firebase-functions';
import cors from 'cors';
import express from 'express';
import helmet from 'helmet';
import type Express from 'express';
import {functionsConfig} from './functions-config.js';
import {handleRequest} from './handle-request.js';

// CORS configuration.
const options: cors.CorsOptions = {
  origin: functionsConfig.whitelist,
};
const VERSION = 'v1';
const app = express();
app.use(helmet());
app.use(cors(options));

app.post(
  `/${VERSION}/mirror/:op`,
  async (
    req: Express.Request,
    res: Express.Response,
    next: Express.NextFunction,
  ) => {
    await handleRequest(req, res, next);
  },
);

app.get(
  `/${VERSION}/mirror/heartbeat`,
  async (req: Express.Request, res: Express.Response) => {
    res.status(200).send('OK');
  },
);

app.listen(3000, () => {
  console.log('Server started on port 3000');
});

export const api = functions.https.onRequest(app);
