import {getMeter} from './view-syncer-instruments.ts';

function createCounter(name: string, options: {description: string}) {
  return getMeter().createCounter(name, {
    description: options.description,
  });
}

export function replicationEvents() {
  return createCounter('replication-events', {
    description: 'Number of replication events processed',
  });
}

export function crudMutations() {
  return createCounter('crud-mutations', {
    description: 'Number of CRUD mutations processed',
  });
}

export function customMutations() {
  return createCounter('custom-mutations', {
    description: 'Number of custom mutations processed',
  });
}

export function pushes() {
  return createCounter('pushes', {
    description: 'Number of pushes processed by the pusher',
  });
}

export function queryHydrations() {
  return createCounter('query-hydrations', {
    description: 'Number of query hydrations',
  });
}

export function cvrRowsFlushed() {
  return createCounter('cvr-rows-flushed', {
    description: 'Number of rows flushed to all CVRs',
  });
}

export function rowsPoked() {
  return createCounter('rows-poked', {
    description: 'Number of rows poked',
  });
}

export function pokeTransactions() {
  return createCounter('poke-transactions', {
    description: 'Number of poke transactions (pokeStart,pokeEnd) pairs',
  });
}
