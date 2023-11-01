import {FieldValue} from '@google-cloud/firestore';
import type {Firestore} from 'firebase-admin/firestore';
import {
  deleteVarsRequestSchema,
  deleteVarsResponseSchema,
} from 'mirror-protocol/src/vars.js';
import {appDataConverter, appPath} from 'mirror-schema/src/app.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';
import {SERVER_VAR_PREFIX, deploymentAfter} from './shared.js';

export const deleteFn = (firestore: Firestore) =>
  validateSchema(deleteVarsRequestSchema, deleteVarsResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore))
    .handle(async (request, context) => {
      const {appID, vars} = request;
      const {
        app: {secrets, runningDeployment},
      } = context;

      const secretNames: string[] = [];
      for (const name of vars) {
        const secretName = `${SERVER_VAR_PREFIX}${name}`;
        if (secrets[secretName]) {
          secretNames.push(secretName);
        }
      }

      if (secretNames.length === 0) {
        return {success: true}; // Nothing to delete.
      }

      const deletedSecrets = {
        ...Object.fromEntries(
          secretNames.map(
            name => [name, FieldValue.delete()] as [string, FieldValue],
          ),
        ),
      };
      const result = await firestore
        .doc(appPath(appID))
        .withConverter(appDataConverter)
        .set(
          {secrets: deletedSecrets},
          {mergeFields: secretNames.map(name => `secrets.${name}`)},
        );
      if (!runningDeployment) {
        // No deployment to re-deploy.
        return {success: true};
      }
      return deploymentAfter(firestore, appID, result.writeTime);
    });
