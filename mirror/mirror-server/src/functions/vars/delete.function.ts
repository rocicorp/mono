import type {Firestore} from 'firebase-admin/firestore';
import {FieldValue} from 'firebase-admin/firestore';
import {
  deleteVarsRequestSchema,
  deleteVarsResponseSchema,
} from 'mirror-protocol/src/vars.js';
import {DEFAULT_ENV, envDataConverter, envPath} from 'mirror-schema/src/env.js';
import {SERVER_VARIABLE_PREFIX} from 'mirror-schema/src/vars.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {getDataOrFail} from '../validators/data.js';
import {validateSchema} from '../validators/schema.js';
import {userAgentVersion} from '../validators/version.js';
import {deploymentAtOrAfter} from './shared.js';

export const deleteFn = (firestore: Firestore) =>
  validateSchema(deleteVarsRequestSchema, deleteVarsResponseSchema)
    .validate(userAgentVersion())
    .validate(userAuthorization())
    .validate(appAuthorization(firestore))
    .handle(async (request, context) => {
      const {appID, vars} = request;
      const {
        app: {runningDeployment},
      } = context;

      const envDoc = firestore
        .doc(envPath(appID, DEFAULT_ENV))
        .withConverter(envDataConverter);

      const {secrets} = getDataOrFail(
        await envDoc.get(),
        'internal',
        `Missing environment for App ${appID}`,
      );

      const secretNames: string[] = [];
      for (const name of vars) {
        const secretName = `${SERVER_VARIABLE_PREFIX}${name}`;
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
      const result = await envDoc.set(
        {secrets: deletedSecrets},
        {mergeFields: secretNames.map(name => `secrets.${name}`)},
      );
      if (!runningDeployment) {
        // No deployment to re-deploy.
        return {success: true};
      }
      return deploymentAtOrAfter(firestore, appID, result.writeTime);
    });
