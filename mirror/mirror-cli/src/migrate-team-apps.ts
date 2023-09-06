import {getFirestore} from 'firebase-admin/firestore';
import {
  teamPath,
  teamDataConverter,
  teamSubdomainIndexPath,
  teamSubdomainIndexDataConverter,
  appNameIndexPath,
  appNameIndexDataConverter,
  sanitizeForSubdomain,
} from 'mirror-schema/src/team.js';
import {appDataConverter, APP_COLLECTION} from 'mirror-schema/src/app.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {must} from 'shared/src/must.js';

export function migrateTeamAppsOptions(yargs: CommonYargsArgv) {
  return yargs
    .option('teamID', {
      describe: 'Team ID to migrate',
      type: 'string',
      demandOption: true,
    })
    .option('team-name', {
      describe: 'Name to give the team, from which Team subdomain is derived.',
      type: 'string',
      demandOption: true,
    });
}

type MigrateTeamAppsHandlerArgs = YargvToInterface<
  ReturnType<typeof migrateTeamAppsOptions>
>;

export async function migrateTeamAppsHandler(
  yargs: MigrateTeamAppsHandlerArgs,
) {
  const {teamID, teamName} = yargs;
  const firestore = getFirestore();
  await firestore.runTransaction(async txn => {
    const teamDoc = await txn.get(
      firestore.doc(teamPath(teamID)).withConverter(teamDataConverter),
    );
    if (!teamDoc.exists) {
      throw new Error(`Team ${teamID} does not exist`);
    }
    const team = must(teamDoc.data());
    if (team.name) {
      throw new Error(`Team ${teamID} is already named ${team.name}`);
    }
    const subdomain = sanitizeForSubdomain(teamName);

    const apps = await txn.get(
      firestore
        .collection(APP_COLLECTION)
        .withConverter(appDataConverter)
        .where('teamID', '==', teamID),
    );
    for (const doc of apps.docs) {
      const app = doc.data();
      // Old global appNames index.
      const oldNameEntry = firestore.doc(`appNames/${app.name}`);
      // New team-scoped appNames index.
      const newNameEntry = firestore
        .doc(appNameIndexPath(teamID, app.name))
        .withConverter(appNameIndexDataConverter);
      txn.delete(oldNameEntry);
      txn.create(newNameEntry, {appID: doc.id});
      txn.update(doc.ref, {teamSubdomain: subdomain});
    }
    txn.update(teamDoc.ref, {
      name: teamName,
      subdomain,
    });
    txn.create(
      firestore
        .doc(teamSubdomainIndexPath(subdomain))
        .withConverter(teamSubdomainIndexDataConverter),
      {teamID},
    );
    console.info(
      `Updating ${apps.size} apps with subdomain ${subdomain} for team ${teamName} (${teamID})`,
    );
  });
}
