import {doc, type Firestore} from 'firebase/firestore';
import {deploymentViewDataConverter} from 'mirror-schema/src/external/deployment.js';
import {watchDoc} from 'mirror-schema/src/external/watch.js';

export async function watchDeployment(
  firestore: Firestore,
  deploymentPath: string,
  completedAction: string,
): Promise<void> {
  const deploymentDoc = doc(firestore, deploymentPath).withConverter(
    deploymentViewDataConverter,
  );

  for await (const snapshot of watchDoc(deploymentDoc)) {
    const deployment = snapshot.data();
    if (!deployment) {
      console.error(`Deployment not found`);
      break;
    }
    if (deployment?.status === 'RUNNING') {
      console.log(`🎁 ${completedAction} successfully to:`);
      console.log(`https://${deployment.spec.hostname}`);
      break;
    }
    console.info(
      `Status: ${deployment.status}${
        deployment.statusMessage ? ': ' + deployment.statusMessage : ''
      }`,
    );
    if (deployment.status === 'FAILED' || deployment.status === 'STOPPED') {
      break;
    }
  }
}
