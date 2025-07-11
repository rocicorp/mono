import type {Histogram} from '@opentelemetry/api';
import {cache, getMeter} from './view-syncer-instruments.ts';

const getOrCreate = cache<Histogram>();

function getOrCreateHistogram(name: string, description: string) {
  return getOrCreate(name, name => {
    const options: {description: string; unit: string; boundaries?: number[]} =
      {
        description,
        unit: 'milliseconds',
      };

    return getMeter().createHistogram(name, options);
  });
}

export function wsMessageProcessingTime() {
  return getOrCreateHistogram(
    'ws-message-processing-time',
    'Time to process a websocket message. The `message.type` attribute is set in order to filter by message type.',
  );
}

export function replicationEventProcessingTime() {
  return getOrCreateHistogram(
    'replication-event-processing-time',
    'Time to process a replication event.',
  );
}

export function transactionAdvanceTime() {
  return getOrCreateHistogram(
    'cg-advance-time',
    'Time to advance all queries for a given client group after applying a new transaction to the replica.',
  );
}

export function changeAdvanceTime() {
  return getOrCreateHistogram(
    'change-advance-time',
    'Time to advance all queries for a given client group for in response to a single change.',
  );
}

export function cvrFlushTime() {
  return getOrCreateHistogram(
    'cvr-flush-time',
    'Time to flush a CVR transaction.',
  );
}

export function pokeTime() {
  return getOrCreateHistogram(
    'poke-flush-time',
    'Time to poke to all clients.',
  );
}

export function hydrationTime() {
  return getOrCreateHistogram('hydration-time', 'Time to hydrate a query.');
}
