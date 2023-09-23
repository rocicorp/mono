import {getFirestore} from 'firebase-admin/firestore';
import {
  TEAM_COLLECTION,
  teamDataConverter,
  teamLabelIndexDataConverter,
} from 'mirror-schema/src/team.js';
import {appDataConverter, APP_COLLECTION} from 'mirror-schema/src/app.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {assert} from 'shared/src/asserts.js';
import {DEPRECATED_TEAM_SUBDOMAIN_INDEX_COLLECTION} from './migrate-team-labels.js';

export function restoreTeamSubdomainsOptions(yargs: CommonYargsArgv) {
  return yargs.option('dry-run', {
    desc: 'Print what would be done but do not commit.',
    type: 'boolean',
    default: true,
  });
}

type RestoreTeamSubdomainsHandlerArgs = YargvToInterface<
  ReturnType<typeof restoreTeamSubdomainsOptions>
>;

export async function restoreTeamSubdomainsHandler(
  yargs: RestoreTeamSubdomainsHandlerArgs,
) {
  const firestore = getFirestore();
  await firestore.runTransaction(async txn => {
    const [teams, indexes, apps] = await Promise.all([
      txn.get(
        firestore.collection(TEAM_COLLECTION).withConverter(teamDataConverter),
      ),
      txn.get(
        firestore
          .collection(DEPRECATED_TEAM_SUBDOMAIN_INDEX_COLLECTION)
          .withConverter(teamLabelIndexDataConverter),
      ),
      txn.get(
        firestore.collection(APP_COLLECTION).withConverter(appDataConverter),
      ),
    ]);
    indexes.docs.forEach(doc => {
      console.log(`Deleting ${doc.ref.path}`);
      txn.delete(doc.ref);
    });
    teams.docs.forEach(doc => {
      const {label} = doc.data();
      assert(label, `Team doc at ${doc.ref.path} does not have a label`);

      console.log(
        `Writing Team(${doc.id}) subdomain field with label ${label}`,
      );
      txn.update(doc.ref, {subdomain: label});
    });
    apps.docs.forEach(doc => {
      const {teamLabel} = doc.data();
      assert(teamLabel, `App doc at ${doc.ref.path} does not have a teamLabel`);

      console.log(
        `Writing app(${doc.id}) teamSubdomain field with ${teamLabel}`,
      );
      txn.update(doc.ref, {teamSubdomain: teamLabel});
    });

    if (yargs.dryRun) {
      throw new Error('Aborted. Set --dry-run=false to commit.');
    }
  });
}
