import {doc, type Firestore} from 'firebase/firestore';
import {deploymentViewDataConverter} from 'mirror-schema/src/external/deployment.js';
import {watchDoc} from 'mirror-schema/src/external/watch.js';

type OutputFormat = 'json' | string | undefined;

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
  output?: OutputFormat,
): Promise<void> {
  const deploymentDoc = doc(firestore, deploymentPath).withConverter(
    deploymentViewDataConverter,
  );
  for await (const snapshot of watchDoc(deploymentDoc)) {
    const deployment = snapshot.data();

    if (!deployment) {
      logError('Deployment not found', output);
      break;
    }

    switch (deployment.status) {
      case 'RUNNING':
        logSuccess(deployment, completedAction, output);
        return;
      case 'FAILED':
      case 'STOPPED':
        logError('Deployment failed', output);
        return;
      default:
        logStatus(deployment, output);
    }
  }
}

function logError(errorMessage: string, outputFormat: OutputFormat) {
  if (outputFormat === 'json') {
    console.log(JSON.stringify({success: false, error: errorMessage}, null, 2));
  } else {
    console.error(errorMessage);
  }
}

function logSuccess(
  deployment: Deployment,
  message: string,
  outputFormat: OutputFormat,
) {
  const successMessage = `https://${deployment.spec.hostname}`;
  if (outputFormat === 'json') {
    console.log(JSON.stringify({success: true, url: successMessage}, null, 2));
  } else {
    console.log(`🎁 ${message} successfully to:`);
    console.log(successMessage);
  }
}

function logStatus(deployment: Deployment, outputFormat: OutputFormat) {
  if (outputFormat !== 'json') {
    console.info(
      `Status: ${deployment.status}${
        deployment.statusMessage ? ': ' + deployment.statusMessage : ''
      }`,
    );
  }
}
