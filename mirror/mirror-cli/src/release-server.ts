import {
  FieldValue,
  Firestore,
  Timestamp,
  getFirestore,
} from 'firebase-admin/firestore';
import {serverPath, serverDataConverter} from 'mirror-schema/src/server.js';
import {
  APP_DEPLOYMENTS_COLLECTION_ID,
  deploymentDataConverter,
} from 'mirror-schema/src/deployment.js';
import {watch, TimeoutError} from 'mirror-schema/src/watch.js';
import type {CommonYargsArgv, YargvToInterface} from './yarg-types.js';

export function releaseReflectServerOptions(yargs: CommonYargsArgv) {
  return yargs
    .option('server', {
      describe:
        'The version of the server to release. This must have already been uploaded via `uploadServer`',
      type: 'string',
      demandOption: true,
    })
    .option('channels', {
      describe:
        'The channels to which the server is to be deployed. This will not change any channels to which it is already deployed.',
      type: 'array',
      string: true,
      default: ['stable'],
    });
}

type ReleaseReflectServerHandlerArgs = YargvToInterface<
  ReturnType<typeof releaseReflectServerOptions>
>;

export async function releaseReflectServerHandler(
  yargs: ReleaseReflectServerHandlerArgs,
) {
  const firestore = getFirestore();
  const serverDoc = firestore
    .doc(serverPath(yargs.server))
    .withConverter(serverDataConverter);
  const {writeTime} = await serverDoc.update({
    channels: FieldValue.arrayUnion(...yargs.channels),
  });
  const server = await serverDoc.get();
  console.log(
    `Server version ${yargs.server} released to [${
      yargs.channels
    }]. All channels: [${server.data()?.channels}]`,
  );
  await watchForDeployments(firestore, writeTime);
}

export const revertReflectServerOptions = releaseReflectServerOptions;

type RevertReflectServerHandlerArgs = YargvToInterface<
  ReturnType<typeof revertReflectServerOptions>
>;

export async function revertReflectServerHandler(
  yargs: RevertReflectServerHandlerArgs,
) {
  const firestore = getFirestore();
  const serverDoc = firestore
    .doc(serverPath(yargs.server))
    .withConverter(serverDataConverter);
  const {writeTime} = await serverDoc.update({
    channels: FieldValue.arrayRemove(...yargs.channels),
  });
  const server = await serverDoc.get();
  console.log(
    `Server version ${yargs.server} removed from ${
      yargs.channels
    }. Remaining channels: ${server.data()?.channels}`,
  );
  await watchForDeployments(firestore, writeTime);
}

const WATCH_DEPLOYMENTS_TIMEOUT = 1000 * 60;

async function watchForDeployments(
  firestore: Firestore,
  from: Timestamp,
): Promise<void> {
  const deployments = firestore
    .collectionGroup(APP_DEPLOYMENTS_COLLECTION_ID)
    .withConverter(deploymentDataConverter)
    .where('type', '==', 'SERVER_UPDATE')
    .where('requestTime', '>', from);
  try {
    console.info(`Watching deployments ...`);
    let last = new RolloutStatus(0);
    for await (const snapshot of watch(
      deployments,
      WATCH_DEPLOYMENTS_TIMEOUT,
    )) {
      const status = new RolloutStatus(snapshot.size);

      snapshot.docs.forEach(doc => {
        switch (doc.data().status) {
          case 'RUNNING':
          case 'STOPPED':
            status.deployed++;
            break;
          case 'FAILED':
            status.failed++;
            break;
        }
      });
      if (!status.equals(last)) {
        status.output();
      }
      last = status;
      if (status.done()) {
        break;
      }
    }
  } catch (e) {
    if (e instanceof TimeoutError) {
      console.warn(`Timed out watching deployments.`);
    } else {
      throw e;
    }
  }
}

class RolloutStatus {
  deployed = 0;
  failed = 0;
  readonly total: number;

  constructor(total: number) {
    this.total = total;
  }

  equals(other: RolloutStatus): boolean {
    return (
      this.total === other.total &&
      this.deployed === other.deployed &&
      this.failed === other.failed
    );
  }

  output() {
    console.info(
      `Deployed to ${this.deployed} of ${this.total} apps (${this.failed} failed)`,
    );
  }

  done() {
    return this.total > 0 && this.deployed + this.failed === this.total;
  }
}
