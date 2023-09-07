import {
  Credential,
  GoogleOAuthAccessToken,
  initializeApp,
} from 'firebase-admin/app';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import color from 'picocolors';
import {getAuth, type Auth} from 'firebase-admin/auth';
import {execSync} from 'node:child_process';

function getProjectID(argv: YargvToInterface<CommonYargsArgv>): string {
  return `reflect-mirror-${argv.stack}`;
}

export function initFirebase(argv: YargvToInterface<CommonYargsArgv>) {
  const projectId = getProjectID(argv);
  initializeApp({
    projectId,
  });
  console.log(`Running on ${color.green(projectId)}\n`);
}

// Unlike all of the other Firebase libraries in the Admin SDK, the Auth library does
// not play well with Application Default Credentials:
//
// https://github.com/firebase/firebase-admin-node/issues/2169
// https://github.com/firebase/firebase-admin-node/issues/1377
// https://github.com/firebase/firebase-admin-node/issues/2106
// https://github.com/firebase/firebase-admin-node/issues/1854
//
// The working alternative is to authenticate as a service account. Rather than generating
// a private key (through the gcp console) and managing a sensitive JSON file to login as
// a service account, we instead use gcloud service account impersonation to generate an
// ephemeral access token that is usable by the Firebase library.
export function getServiceAccountAuth(
  name: string,
  yargs: YargvToInterface<CommonYargsArgv>,
): Auth {
  const projectId = getProjectID(yargs);
  const serviceAccount = `${name}@${projectId}.iam.gserviceaccount.com`;
  const credential: Credential = {
    getAccessToken(): Promise<GoogleOAuthAccessToken> {
      const token = String(
        execSync(
          `gcloud auth --impersonate-service-account ${serviceAccount} print-access-token`,
          {stdio: ['ignore', 'pipe', 'ignore']},
        ),
      ).trim();
      console.log(
        `Generated access token for ${color.blue(serviceAccount)} from gcloud`,
      );
      return Promise.resolve({
        ['access_token']: token,
        ['expires_in']: 600,
      });
    },
  };
  const app = initializeApp(
    {
      credential,
      projectId,
    },
    `${name}-auth`,
  );
  return getAuth(app);
}
