import {expect} from 'chai';
import {mergePokes} from './merge-pokes.js';

test('mergePokes with empty array returns undefined', () => {
  const merged = mergePokes([]);
  expect(merged).to.be.undefined;
});

test('merge multiple pokes', () => {
  const merged = mergePokes([
    {
      baseCookie: 1,
      cookie: 2,
      lastMutationIDChanges: {c1: 2},
      presence: [
        {
          op: 'put',
          key: '123',
          value: 1,
        },
      ],
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 1,
        },
      ],
      timestamp: 100,
    },
    {
      baseCookie: 2,
      cookie: 3,
      lastMutationIDChanges: {c2: 2},
      presence: [],
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 2,
        },
      ],
      timestamp: 120,
    },
    {
      baseCookie: 3,
      cookie: 4,
      lastMutationIDChanges: {c1: 3, c3: 1},
      presence: [
        {
          op: 'del',
          key: '234',
        },
      ],
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 3,
        },
      ],
      timestamp: 140,
    },
  ]);
  expect(merged).to.deep.equal({
    baseCookie: 1,
    cookie: 4,
    lastMutationIDChanges: {c1: 3, c2: 2, c3: 1},
    presence: [
      {
        op: 'put',
        key: '123',
        value: 1,
      },
      {
        op: 'del',
        key: '234',
      },
    ],
    patch: [
      {
        op: 'put',
        key: 'count',
        value: 1,
      },
      {
        op: 'put',
        key: 'count',
        value: 2,
      },
      {
        op: 'put',
        key: 'count',
        value: 3,
      },
    ],
    timestamp: 100,
  });
});

test('merge multiple pokes no presence', () => {
  const merged = mergePokes([
    {
      baseCookie: 1,
      cookie: 2,
      lastMutationIDChanges: {c1: 2},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 1,
        },
      ],
      timestamp: 100,
    },
    {
      baseCookie: 2,
      cookie: 3,
      lastMutationIDChanges: {c2: 2},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 2,
        },
      ],
      timestamp: 120,
    },
    {
      baseCookie: 3,
      cookie: 4,
      lastMutationIDChanges: {c1: 3, c3: 1},
      patch: [
        {
          op: 'put',
          key: 'count',
          value: 3,
        },
      ],
      timestamp: 140,
    },
  ]);
  expect(merged).to.deep.equal({
    baseCookie: 1,
    cookie: 4,
    lastMutationIDChanges: {c1: 3, c2: 2, c3: 1},
    presence: [],
    patch: [
      {
        op: 'put',
        key: 'count',
        value: 1,
      },
      {
        op: 'put',
        key: 'count',
        value: 2,
      },
      {
        op: 'put',
        key: 'count',
        value: 3,
      },
    ],
    timestamp: 100,
  });
});
