import type {Firestore} from './firebase.js';
import type {App as SchemaApp} from 'mirror-schema/src/app.js';
import {APP_COLLECTION, appDataConverter} from 'mirror-schema/src/app.js';
import {userDataConverter, userPath} from 'mirror-schema/src/user.js';
import {must} from 'shared/src/must.js';

export type App = SchemaApp & {appID: string};

export function getExistingAppsForUser(
  firestore: Firestore,
  userID: string,
): Promise<App[]> {
  const userDocRef = firestore
    .doc(userPath(userID))
    .withConverter(userDataConverter.forClient);

  return firestore.runTransaction(async txn => {
    const userDoc = await txn.get(userDocRef);
    if (!userDoc.exists) {
      throw new Error('User does not exist');
    }

    const user = must(userDoc.data());
    const teamIDs = Object.keys(user.roles);
    if (teamIDs.length === 0) {
      return [];
    }

    const appsSnapshot = await firestore
      .collection(APP_COLLECTION)
      .withConverter(appDataConverter.forClient)
      .where('teamID', 'in', teamIDs)
      .get();

    const apps: App[] = [];
    appsSnapshot.forEach(app => {
      const data = app.data();
      apps.push({...data, appID: app.id});
    });
    return apps;
  });
}
