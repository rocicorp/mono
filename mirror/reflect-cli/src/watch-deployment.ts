import {doc, type Firestore} from 'firebase/firestore';
import {deploymentViewDataConverter} from 'mirror-schema/src/external/deployment.js';
import {watchDoc} from 'mirror-schema/src/external/watch.js';
import {getLogger} from './logger.js';
import type {DeploymentView} from 'mirror-schema/src/external/deployment.js';

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
      throw new Error('Deployment not found');
    }

    switch (deployment.status) {
      case 'RUNNING':
        logSuccess(deployment, completedAction);
        return;
      case 'FAILED':
      case 'STOPPED':
        throw Error('Deployment failed');
      default:
        logStatus(deployment);
    }
  }
}

function logSuccess(deployment: DeploymentView, message: string) {
  const url = `https://${deployment.spec.hostname}`;
  getLogger().json({success: true, url});
  getLogger().log(`🎁 ${message} successfully to:`);
  getLogger().log(url);
}

function logStatus(deployment: DeploymentView) {
  getLogger().info(
    `Status: ${deployment.status}${
      deployment.statusMessage ? ': ' + deployment.statusMessage : ''
    }`,
  );
}
