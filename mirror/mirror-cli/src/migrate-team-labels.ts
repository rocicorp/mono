import {getFirestore} from 'firebase-admin/firestore';
import {
  TEAM_COLLECTION,
  sanitizeForLabel,
  teamDataConverter,
  teamLabelIndexPath,
  teamLabelIndexDataConverter,
} from 'mirror-schema/src/team.js';
import {appDataConverter, APP_COLLECTION} from 'mirror-schema/src/app.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {assert} from 'shared/src/asserts.js';

export const DEPRECATED_TEAM_SUBDOMAIN_INDEX_COLLECTION = 'teamSubdomains';

export function migrateTeamLabelsOptions(yargs: CommonYargsArgv) {
  return yargs.option('dry-run', {
    desc: 'Print what would be done but do not commit.',
    type: 'boolean',
    default: true,
  });
}

type MigrateTeamLabelsHandlerArgs = YargvToInterface<
  ReturnType<typeof migrateTeamLabelsOptions>
>;

export async function migrateTeamLabelsHandler(
  yargs: MigrateTeamLabelsHandlerArgs,
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
    const labels = new Set<string>();
    indexes.docs.forEach(doc => {
      const subdomain = doc.id;
      const label = sanitizeForLabel(subdomain);

      assert(!labels.has(label), `Already seen label ${label}`);
      labels.add(label);

      const newIndex = firestore
        .doc(teamLabelIndexPath(label))
        .withConverter(teamLabelIndexDataConverter);

      console.log(`Copying ${doc.ref.path} to ${newIndex.path}`);
      txn.set(newIndex, doc.data());
    });
    teams.docs.forEach(doc => {
      const {subdomain} = doc.data();
      if (!subdomain) {
        const {label} = doc.data();
        console.log(`Team ${doc.id} is already migrated to label ${label}.`);
        return;
      }
      const label = sanitizeForLabel(subdomain);
      assert(
        labels.has(label),
        `Did not see index entry for Team ${label} (${subdomain})`,
      );

      console.log(`Setting team(${doc.id}) label ${label} from ${subdomain}`);
      txn.update(doc.ref, {label});
    });
    apps.docs.forEach(doc => {
      const {teamSubdomain} = doc.data();
      if (!teamSubdomain) {
        const {teamLabel} = doc.data();
        console.log(
          `Team ${doc.id} is already migrated to label ${teamLabel}.`,
        );
        return;
      }
      const teamLabel = sanitizeForLabel(teamSubdomain);
      assert(
        labels.has(teamLabel),
        `Did not see index entry for App with Team ${teamLabel} (${teamSubdomain})`,
      );

      console.log(
        `Setting app(${doc.id}) teamLabel ${teamLabel} from ${teamSubdomain}`,
      );
      txn.update(doc.ref, {teamLabel});
    });

    if (yargs.dryRun) {
      throw new Error('Aborted. Set --dry-run=false to commit.');
    }
  });
}
