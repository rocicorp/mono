import {getProviderConfig} from './cf.js';
import {GlobalScript} from 'cloudflare-api/src/scripts.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';
import {
  DocumentReference,
  Firestore,
  Timestamp,
  getFirestore,
} from 'firebase-admin/firestore';
import {appPath, appDataConverter} from 'mirror-schema/src/app.js';
import {
  deploymentsCollection,
  deploymentDataConverter,
  Deployment,
} from 'mirror-schema/src/deployment.js';
import {watch} from 'mirror-schema/src/watch.js';
import {unreachable} from 'shared/src/asserts.js';

export function migrateToWFPOptions(yargs: CommonYargsArgv) {
  return yargs.positional('appID', {
    desc: 'The id of the app to migrate',
    type: 'string',
    demandOption: true,
  });
}

type MigrateToWFPArgs = YargvToInterface<
  ReturnType<typeof migrateToWFPOptions>
>;

export async function migrateToWFPHandler(
  yargs: MigrateToWFPArgs,
): Promise<void> {
  const {stack, appID} = yargs;
  const firestore = getFirestore();
  const appDoc = firestore.doc(appPath(appID)).withConverter(appDataConverter);
  const app = (await appDoc.get()).data();
  if (!app) {
    throw new Error(
      `App ${appID} not found. Did you specify the correct --stack?`,
    );
  }
  const {scriptRef, cfScriptName: name} = app;
  if (scriptRef) {
    throw new Error(`App is already migrated to ${scriptRef.namespace}`);
  }

  // The old script must first be deleted because:
  // - There is a naming collision bug in Cloudflare that prevents same-named scripts
  //   (even across namespaces) from being created.
  // - The CNAME for Custom Hostnames cannot be created if a Custom Domain already exists
  //   for that hostname.
  console.log(`Deleting script ${name} and any custom domains`);
  const config = await getProviderConfig(yargs);
  await new GlobalScript(config, name).delete();

  const newScriptRef = {
    namespace: stack === 'prod' ? 'prod' : 'sand',
    name,
  };

  console.log(`Setting ScriptRef`, newScriptRef);
  const {writeTime} = await appDoc.update({
    scriptRef: newScriptRef,
    forceRedeployment: true, // Needed because the scriptRef does not affect the DeploymentSpec.
  });

  const deployment = await getNewDeployment(firestore, appID, writeTime);
  let lastStatus;
  for await (const doc of watch(deployment)) {
    const state = doc.data();
    if (state === undefined) {
      throw new Error(`Deployment deleted?`);
    }
    const {status, statusMessage} = state;
    if (status !== lastStatus) {
      console.info(
        `Status: ${status}${statusMessage ? ': ' + statusMessage : ''}`,
      );
      lastStatus = status;
    }
    switch (status) {
      case 'RUNNING':
      case 'STOPPED':
      case 'FAILED':
        return;
    }
  }
}

async function getNewDeployment(
  firestore: Firestore,
  appID: string,
  since: Timestamp,
): Promise<DocumentReference<Deployment>> {
  const newDeployments = firestore
    .collection(deploymentsCollection(appID))
    .withConverter(deploymentDataConverter)
    .where('requestTime', '>', since);

  console.log(`Watching new deployments ...`);
  for await (const deployments of watch(newDeployments)) {
    if (deployments.docs.length) {
      return deployments.docs[0].ref;
    }
  }
  unreachable();
}
