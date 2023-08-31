import type {Firestore} from 'firebase-admin/firestore';
import {HttpsError} from 'firebase-functions/v2/https';
import {
  renameAppRequestSchema,
  renameAppResponseSchema,
} from 'mirror-protocol/src/app.js';
import {
  appDataConverter,
  appPath,
  isValidAppName,
} from 'mirror-schema/src/app.js';
import {
  appNameIndexDataConverter,
  appNameIndexPath,
} from 'mirror-schema/src/team.js';
import {must} from 'shared/src/must.js';
import {appAuthorization, userAuthorization} from '../validators/auth.js';
import {validateSchema} from '../validators/schema.js';
import {logger} from 'firebase-functions';

export const rename = (firestore: Firestore) =>
  validateSchema(renameAppRequestSchema, renameAppResponseSchema)
    .validate(userAuthorization())
    .validate(appAuthorization(firestore))
    .handle(async (request, context) => {
      const {appID, name: newName} = request;

      if (!isValidAppName(newName)) {
        throw new HttpsError(
          'invalid-argument',
          `Invalid App Name "${newName}". Names must be lowercased alphanumeric, starting with a letter and not ending with a hyphen`,
        );
      }

      const appDocRef = firestore
        .doc(appPath(appID))
        .withConverter(appDataConverter);

      await firestore.runTransaction(async txn => {
        // Note: Although the app has already been looked up once in appValidation(),
        // it's more straightforward to have the transaction logic rely only on
        // data read within the transaction.
        const appDoc = await txn.get(appDocRef);
        if (!appDoc.exists) {
          throw new HttpsError(
            'not-found',
            `App ${appID} was concurrently deleted.`,
          );
        }
        const app = must(appDoc.data());
        if (app.name === newName) {
          // This app was already renamed to the new name. No-op success.
          logger.info(`App ${appID} already has ${newName}`);
          return;
        }
        const oldAppNameDocRef = firestore
          .doc(appNameIndexPath(app.teamID, app.name))
          .withConverter(appNameIndexDataConverter);
        const newAppNameDocRef = firestore
          .doc(appNameIndexPath(app.teamID, newName))
          .withConverter(appNameIndexDataConverter);
        const [oldAppNameDoc, newAppNameDoc] = await Promise.all([
          txn.get(oldAppNameDocRef),
          txn.get(newAppNameDocRef),
        ]);

        if (newAppNameDoc.exists) {
          throw new HttpsError(
            'already-exists',
            `An app with the name ${newName} already exists`,
          );
        }
        // Sanity check.
        if (oldAppNameDoc.data()?.appID !== appID) {
          throw new HttpsError(
            'internal',
            `Data for ${appID} is corrupt`,
            oldAppNameDoc.data(),
          );
        }
        txn.delete(oldAppNameDocRef);
        txn.create(newAppNameDocRef, {appID});
        txn.update(appDocRef, {name: newName});
      });

      logger.info(`Renamed ${appID} from ${context.app.name} to ${newName}`);
      return {success: true};
    });
