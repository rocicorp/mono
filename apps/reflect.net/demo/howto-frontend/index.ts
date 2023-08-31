import {getWorkerHost} from '@/util/worker-host';
import {Reflect} from '@rocicorp/reflect/client';
import {loggingOptions} from '../frontend/logging-options';
import {M, mutators} from '../shared/mutators';

export const init = (roomID: string, userID: string): Reflect<M> => {
  // Set up our connection to reflect
  // Create a reflect client
  const reflectClient = new Reflect<M>({
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
