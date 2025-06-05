import type {UpDownCounter} from '@opentelemetry/api';
import {cache, getMeter} from './view-syncer-instruments.ts';

const getOrCreateUpDownCounter = cache<UpDownCounter>();
function createUpDownCounter(name: string, options: {description: string}) {
  return getMeter().createUpDownCounter(name, {
    description: options.description,
  });
}

export function activeConnections() {
  return getOrCreateUpDownCounter('active-connections', name =>
    createUpDownCounter(name, {
      description: 'Number of active websocket connections',
    }),
  );
}

export function activeQueries() {
  return getOrCreateUpDownCounter('active-queries', name =>
    createUpDownCounter(name, {
      description: 'Number of active queries',
    }),
  );
}

export function activeClients() {
  return getOrCreateUpDownCounter('active-clients', name =>
    createUpDownCounter(name, {
      description: 'Number of active clients',
    }),
  );
}

export function activeClientGroups() {
  return getOrCreateUpDownCounter('active-client-groups', name =>
    createUpDownCounter(name, {
      description: 'Number of active client groups',
    }),
  );
}

export function activeViewSyncerInstances() {
  return getOrCreateUpDownCounter('active-view-syncer-instances', name =>
    createUpDownCounter(name, {
      description: 'Number of active view syncer instances',
    }),
  );
}

export function activePusherInstances() {
  return getOrCreateUpDownCounter('active-pusher-instances', name =>
    createUpDownCounter(name, {
      description: 'Number of active pusher instances',
    }),
  );
}

export function activeIvmStorageInstances() {
  return getOrCreateUpDownCounter('active-ivm-storage-instances', name =>
    createUpDownCounter(name, {
      description: 'Number of active ivm operator storage instances',
    }),
  );
}
