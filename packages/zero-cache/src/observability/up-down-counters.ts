import type {UpDownCounter} from '@opentelemetry/api';
import {cache, getMeter} from './view-syncer-instruments.ts';

const getOrCreate = cache<UpDownCounter>();

function getOrCreateUpDownCounter(name: string, description: string) {
  return getOrCreate(name, name =>
    getMeter().createUpDownCounter(name, {description}),
  );
}

export function activeConnections() {
  return getOrCreateUpDownCounter(
    'active-connections',
    'Number of active websocket connections',
  );
}

export function activeQueries() {
  return getOrCreateUpDownCounter('active-queries', 'Number of active queries');
}

export function activeClients() {
  return getOrCreateUpDownCounter('active-clients', 'Number of active clients');
}

export function activeClientGroups() {
  return getOrCreateUpDownCounter(
    'active-client-groups',
    'Number of active client groups',
  );
}

export function activeViewSyncerInstances() {
  return getOrCreateUpDownCounter(
    'active-view-syncer-instances',
    'Number of active view syncer instances',
  );
}

export function activePusherInstances() {
  return getOrCreateUpDownCounter(
    'active-pusher-instances',
    'Number of active pusher instances',
  );
}

export function activeIvmStorageInstances() {
  return getOrCreateUpDownCounter(
    'active-ivm-storage-instances',
    'Number of active ivm operator storage instances',
  );
}
