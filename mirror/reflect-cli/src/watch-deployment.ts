import {doc, type Firestore} from 'firebase/firestore';
import {deploymentViewDataConverter} from 'mirror-schema/src/external/deployment.js';
import {watchDoc} from 'mirror-schema/src/external/watch.js';
import {getLogger} from './logger.js';

interface Deployment {
  status?: string | undefined;
  spec: {
    hostname?: string | undefined;
  };
  statusMessage?: string | undefined;
}

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
      logError('Deployment not found');
      break;
    }

    switch (deployment.status) {
      case 'RUNNING':
        logSuccess(deployment, completedAction);
        return;
      case 'FAILED':
      case 'STOPPED':
        logError('Deployment failed');
        return;
      default:
        logStatus(deployment);
    }
  }
}

function logError(errorMessage: string) {
  getLogger().log(
    JSON.stringify({success: false, error: errorMessage}, null, 2),
  );
  getLogger().error(errorMessage);
}

function logSuccess(deployment: Deployment, message: string) {
  const url = `https://${deployment.spec.hostname}`;
  getLogger().json({success: true, url});
  getLogger().log(`🎁 ${message} successfully to:`);
  getLogger().log(url);
}

function logStatus(deployment: Deployment) {
  getLogger().info(
    `Status: ${deployment.status}${
      deployment.statusMessage ? ': ' + deployment.statusMessage : ''
    }`,
  );
}
