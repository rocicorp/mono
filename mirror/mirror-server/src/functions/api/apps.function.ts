import type {Response} from 'express';
import type {Auth} from 'firebase-admin/auth';
import type {Firestore} from 'firebase-admin/firestore';
import {logger} from 'firebase-functions';
import {HttpsError, type Request} from 'firebase-functions/v2/https';
import type {RequiredPermission} from 'mirror-schema/src/app-key.js';
import type {App} from 'mirror-schema/src/app.js';
import {DEFAULT_ENV, envDataConverter, envPath} from 'mirror-schema/src/env.js';
import {SemVer, lt} from 'semver';
import {API_KEY_HEADER_NAME} from 'shared/src/api/headers.js';
import {APIErrorCode, makeAPIError} from 'shared/src/api/responses.js';
import {
  Secrets,
  SecretsCache,
  type SecretsClient,
} from '../../secrets/index.js';
import {REFLECT_API_KEY, decryptSecrets} from '../app/secrets.js';
import {
  appOrKeyAuthorization,
  authenticatedAsRequester,
  authorizationHeader,
} from '../validators/auth.js';
import {getDataOrFail} from '../validators/data.js';
import {contextValidator} from '../validators/https.js';
import {makeWorkerPath, parseReadParams, parseWriteParams} from './paths.js';

const MIN_VERSION = new SemVer('0.38.202312200000');

export const apps =
  (firestore: Firestore, auth: Auth, secretsClient: SecretsClient) =>
  async (request: Request, response: Response) => {
    try {
      const secrets = new SecretsCache(secretsClient);
      const {appID, permission, workerPath} = parsePath(
        request.method,
        request.path,
      );

      logger.debug(
        `Verifying authorization and ${permission} permission for ${request.url}`,
      );
      const {app} = await contextValidator({appID}, {request})
        .validate(authorizationHeader(firestore, auth))
        .validate(authenticatedAsRequester())
        .validate(appOrKeyAuthorization(firestore, permission))
        .process();

      const hostname = checkDeployment(app);

      const reflectAPIKey = await getReflectAPIKey(
        firestore,
        secrets,
        appID,
        app,
      );

      const queryStart = request.originalUrl.indexOf('?');
      const query =
        queryStart > 0 ? request.originalUrl.substring(queryStart) : '';
      const workerURL = `https://${hostname}${workerPath}${query}`;

      logger.info(`Proxying request: ${request.method} ${workerURL}`);
      const resp = await fetch(`https://${hostname}${workerPath}`, {
        method: request.method,
        headers: {[API_KEY_HEADER_NAME]: reflectAPIKey},
        body: request.rawBody,
      });
      // TODO: There's probably a way to stream/pipe the response bytes back.
      const body = await resp.text();
      logger.debug(`Response ${resp.status} (${body.length} bytes)`, body);
      response
        .status(resp.status)
        .type(resp.status >= 500 ? 'text' : 'json')
        .send(body);
    } catch (e) {
      const err =
        e instanceof HttpsError ? e : new HttpsError('internal', String(e), e);
      const code = err.httpErrorCode.status;
      if (code >= 500) {
        logger.error(e);
        response.status(code).send(err.message);
      } else {
        // 4xx errors are logged as warnings and returned in the APIResponse format.
        logger.warn(e);
        response.status(code).json(
          makeAPIError({
            code: code as APIErrorCode,
            resource: 'request',
            message: err.message,
          }),
        );
      }
    }
  };

function parsePath(
  method: string,
  path: string,
): {appID: string; permission: RequiredPermission; workerPath: string} {
  if (method.toLowerCase() === 'get') {
    const params = parseReadParams(path);
    const permission = `${params.resource}:read` as RequiredPermission;
    return {
      appID: params.appID,
      permission,
      workerPath: makeWorkerPath(params),
    };
  }
  if (method.toLowerCase() === 'post') {
    const params = parseWriteParams(path);
    const permission =
      `${params.resource}:${params.command}` as RequiredPermission;
    return {
      appID: params.appID,
      permission,
      workerPath: makeWorkerPath(params),
    };
  }
  const error = new HttpsError(
    'invalid-argument',
    `Unsupported method "${method}"`,
  );
  // There's no FunctionsErrorCode for 405: Unsupported Method, so we hack it.
  error.httpErrorCode.status = 405;
  throw error;
}

function checkDeployment(app: App): string {
  const {name, runningDeployment} = app;
  if (!runningDeployment) {
    throw new HttpsError('failed-precondition', `App "${name}" is not running`);
  }
  const {
    spec: {hostname, serverVersion},
  } = runningDeployment;

  const version = new SemVer(serverVersion);
  if (lt(version, MIN_VERSION)) {
    throw new HttpsError(
      'failed-precondition',
      `App "${name}" is at server version ${serverVersion} which does not support the REST API.\n` +
        'Update the app to @rocicorp/reflect@latest and re-publish.',
    );
  }
  return hostname;
}

async function getReflectAPIKey(
  firestore: Firestore,
  secrets: Secrets,
  appID: string,
  app: App,
): Promise<string> {
  const env = await firestore
    .doc(envPath(appID, DEFAULT_ENV))
    .withConverter(envDataConverter)
    .get();
  const {secrets: envSecrets} = getDataOrFail(
    env,
    'internal',
    `Missing environment for App ${app.name}`,
  );
  const appSecrets = await decryptSecrets(secrets, {
    [REFLECT_API_KEY]: envSecrets[REFLECT_API_KEY],
  });
  return appSecrets[REFLECT_API_KEY];
}
