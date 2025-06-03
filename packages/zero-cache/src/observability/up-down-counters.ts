import {getMeter} from './view-syncer-instruments.ts';

function createUpDownCounter(name: string, options: {description: string}) {
  return getMeter().createUpDownCounter(name, {
    description: options.description,
  });
}

export function activeConnections() {
  return createUpDownCounter('active-connections', {
    description: 'Number of active websocket connections',
  });
}

export function activeQueries() {
  return createUpDownCounter('active-queries', {
    description: 'Number of active queries',
  });
}

export function activeClients() {
  return createUpDownCounter('active-clients', {
    description: 'Number of active clients',
  });
}

export function activeClientGroups() {
  return createUpDownCounter('active-client-groups', {
    description: 'Number of active client groups',
  });
}

export function activeViewSyncerInstances() {
  return createUpDownCounter('active-view-syncer-instances', {
    description: 'Number of active view syncer instances',
  });
}

export function activePusherInstances() {
  return createUpDownCounter('active-pusher-instances', {
    description: 'Number of active pusher instances',
  });
}

export function activeIvmStorageInstances() {
  return createUpDownCounter('active-ivm-storage-instances', {
    description: 'Number of active ivm operator storage instances',
  });
}
