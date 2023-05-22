import {ExperimentalMemKVStore, Reflect} from '@rocicorp/reflect';
import {loggingOptions} from '../frontend/logging-options';
import {mutators, M} from '../shared/mutators';
import {getWorkerHost} from '@/util/worker-host';

export const init = (roomID: string, userID: string): Reflect<M> => {
  // Set up our connection to reflect
  // Create a reflect client
  const reflectClient = new Reflect<M>({
    createKVStore: name => new ExperimentalMemKVStore(name),
    socketOrigin: getWorkerHost(),
    onOnlineChange: online => {
      console.log(`${userID} online: ${online}`);
    },
    userID,
    roomID,
    auth: JSON.stringify({
      userID,
      roomID,
    }),
    mutators,
    ...loggingOptions,
  });

  reflectClient.onUpdateNeeded = reason => {
    if (reason.type !== 'NewClientGroup') {
      location.reload();
    }
  };

  return reflectClient;
};
