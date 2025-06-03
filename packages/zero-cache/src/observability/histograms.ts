import {getMeter} from './view-syncer-instruments.ts';

function createHistogram(
  name: string,
  options: {description: string; unit?: string},
) {
  return getMeter().createHistogram(name, options);
}

export function wsMessageProcessingTime() {
  return createHistogram('ws-message-processing-time', {
    description:
      'Time to process a websocket message. The `message.type` attribute is set in order to filter by message type.',
    unit: 'milliseconds',
  });
}

export function replicationEventProcessingTime() {
  return createHistogram('replication-event-processing-time', {
    description: 'Time to process a replication event.',
    unit: 'milliseconds',
  });
}

export function transactionAdvanceTime() {
  return createHistogram('cg-advance-time', {
    description:
      'Time to advance all queries for a given client group after applying a new transaction to the replica.',
    unit: 'milliseconds',
  });
}

export function changeAdvanceTime() {
  return createHistogram('change-advance-time', {
    description:
      'Time to advance all queries for a given client group for in response to a single change.',
    unit: 'milliseconds',
  });
}

export function cvrFlushTime() {
  return createHistogram('cvr-flush-time', {
    description: 'Time to flush a CVR transaction.',
    unit: 'milliseconds',
  });
}

export function pokeTime() {
  return createHistogram('poke-flush-time', {
    description: 'Time to poke to all clients.',
    unit: 'milliseconds',
  });
}

export function hydrationTime() {
  return createHistogram('hydration-time', {
    description: 'Time to hydrate a query.',
    unit: 'milliseconds',
  });
}
