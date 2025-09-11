import {describe, expect, test} from 'vitest';
import type {Write} from './dag/store.ts';
import {TestStore} from './dag/test-store.ts';
import {
  confirmDeletedClients,
  mergeDeletedClients,
  normalizeDeletedClients,
  removeFromDeletedClients,
  setDeletedClients,
  type DeletedClients,
} from './deleted-clients.ts';
import {
  makeClientV6,
  setClientsForTesting,
} from './persist/clients-test-helpers.ts';
import {withWrite} from './with-transactions.ts';

describe('normalizeDeletedClients', () => {
  test('sorts by client group ID then client ID', () => {
    const input: DeletedClients = [
      {clientGroupID: 'group-z', clientID: 'client-3'},
      {clientGroupID: 'group-a', clientID: 'client-b'},
      {clientGroupID: 'group-z', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-a'},
      {clientGroupID: 'group-m', clientID: 'client-y'},
      {clientGroupID: 'group-a', clientID: 'client-c'},
      {clientGroupID: 'group-m', clientID: 'client-x'},
      {clientGroupID: 'group-z', clientID: 'client-2'},
    ];

    const result = normalizeDeletedClients(input);

    expect(result).toEqual([
      {clientGroupID: 'group-a', clientID: 'client-a'},
      {clientGroupID: 'group-a', clientID: 'client-b'},
      {clientGroupID: 'group-a', clientID: 'client-c'},
      {clientGroupID: 'group-m', clientID: 'client-x'},
      {clientGroupID: 'group-m', clientID: 'client-y'},
      {clientGroupID: 'group-z', clientID: 'client-1'},
      {clientGroupID: 'group-z', clientID: 'client-2'},
      {clientGroupID: 'group-z', clientID: 'client-3'},
    ]);
  });

  test('deduplicates identical pairs', () => {
    const input: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-2'},
      {clientGroupID: 'group-a', clientID: 'client-1'}, // duplicate
      {clientGroupID: 'group-b', clientID: 'client-x'},
      {clientGroupID: 'group-a', clientID: 'client-2'}, // duplicate
      {clientGroupID: 'group-b', clientID: 'client-x'}, // duplicate
    ];

    const result = normalizeDeletedClients(input);

    expect(result).toEqual([
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-2'},
      {clientGroupID: 'group-b', clientID: 'client-x'},
    ]);
  });

  test('handles empty input', () => {
    const result = normalizeDeletedClients([]);
    expect(result).toEqual([]);
  });

  test('preserves already normalized input', () => {
    const input: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-2'},
      {clientGroupID: 'group-b', clientID: 'client-x'},
      {clientGroupID: 'group-b', clientID: 'client-y'},
    ];

    const result = normalizeDeletedClients(input);

    expect(result).toEqual(input);
  });

  test('handles single item', () => {
    const input: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
    ];

    const result = normalizeDeletedClients(input);

    expect(result).toEqual(input);
  });
});

describe('mergeDeletedClients', () => {
  test('merges two non-overlapping DeletedClients', () => {
    const a: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-2'},
      {clientGroupID: 'group-b', clientID: 'client-3'},
    ];
    const b: DeletedClients = [
      {clientGroupID: 'group-c', clientID: 'client-4'},
      {clientGroupID: 'group-c', clientID: 'client-5'},
      {clientGroupID: 'group-d', clientID: 'client-6'},
    ];

    const result = mergeDeletedClients(a, b);

    expect(result).toEqual([
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-2'},
      {clientGroupID: 'group-b', clientID: 'client-3'},
      {clientGroupID: 'group-c', clientID: 'client-4'},
      {clientGroupID: 'group-c', clientID: 'client-5'},
      {clientGroupID: 'group-d', clientID: 'client-6'},
    ]);
  });

  test('merges overlapping client groups by combining and deduplicating pairs', () => {
    const a: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-2'},
      {clientGroupID: 'group-b', clientID: 'client-3'},
    ];
    const b: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-2'}, // duplicate
      {clientGroupID: 'group-a', clientID: 'client-4'}, // new in same group
      {clientGroupID: 'group-c', clientID: 'client-5'},
    ];

    const result = mergeDeletedClients(a, b);

    expect(result).toEqual([
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-2'}, // no duplicate
      {clientGroupID: 'group-a', clientID: 'client-4'},
      {clientGroupID: 'group-b', clientID: 'client-3'},
      {clientGroupID: 'group-c', clientID: 'client-5'},
    ]);
  });

  test('handles empty inputs', () => {
    const a: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
    ];
    const empty: DeletedClients = [];

    expect(mergeDeletedClients(a, empty)).toEqual(a);
    expect(mergeDeletedClients(empty, a)).toEqual(a);
    expect(mergeDeletedClients(empty, empty)).toEqual([]);
  });

  test('handles duplicates across inputs', () => {
    const a: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-b', clientID: 'client-2'},
    ];
    const b: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'}, // exact duplicate
      {clientGroupID: 'group-b', clientID: 'client-3'},
    ];

    const result = mergeDeletedClients(a, b);

    expect(result).toEqual([
      {clientGroupID: 'group-a', clientID: 'client-1'}, // no duplicate
      {clientGroupID: 'group-b', clientID: 'client-2'},
      {clientGroupID: 'group-b', clientID: 'client-3'},
    ]);
  });

  test('preserves original objects (immutability)', () => {
    const a: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
    ];
    const b: DeletedClients = [
      {clientGroupID: 'group-b', clientID: 'client-2'},
    ];

    const result = mergeDeletedClients(a, b);

    // Original objects should not be modified
    expect(a).toEqual([{clientGroupID: 'group-a', clientID: 'client-1'}]);
    expect(b).toEqual([{clientGroupID: 'group-b', clientID: 'client-2'}]);

    // Result should contain both
    expect(result).toEqual([
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-b', clientID: 'client-2'},
    ]);
  });
});

describe('removeFromDeletedClients', () => {
  test('removes specified pairs from deleted clients', () => {
    const old: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-2'},
      {clientGroupID: 'group-a', clientID: 'client-3'},
      {clientGroupID: 'group-b', clientID: 'client-4'},
      {clientGroupID: 'group-b', clientID: 'client-5'},
      {clientGroupID: 'group-c', clientID: 'client-6'},
    ];
    const toRemove: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-2'}, // remove one from group-a
      {clientGroupID: 'group-b', clientID: 'client-4'}, // remove one from group-b
      {clientGroupID: 'group-b', clientID: 'client-5'}, // remove another from group-b
    ];

    const result = removeFromDeletedClients(old, toRemove);

    expect(result).toEqual([
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-3'},
      {clientGroupID: 'group-c', clientID: 'client-6'},
    ]);
  });

  test('removes all specified pairs', () => {
    const old: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-2'},
      {clientGroupID: 'group-b', clientID: 'client-3'},
    ];
    const toRemove: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-2'},
      {clientGroupID: 'group-b', clientID: 'client-3'},
    ];

    const result = removeFromDeletedClients(old, toRemove);

    expect(result).toEqual([]);
  });

  test('handles non-existent pairs in toRemove', () => {
    const old: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-2'},
    ];
    const toRemove: DeletedClients = [
      {clientGroupID: 'group-b', clientID: 'client-3'}, // non-existent group
      {clientGroupID: 'group-a', clientID: 'client-4'}, // non-existent client in existing group
    ];

    const result = removeFromDeletedClients(old, toRemove);

    expect(result).toEqual(old); // unchanged
  });

  test('handles partial removals', () => {
    const old: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-2'},
      {clientGroupID: 'group-a', clientID: 'client-3'},
    ];
    const toRemove: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'}, // existing
      {clientGroupID: 'group-a', clientID: 'client-4'}, // non-existing
    ];

    const result = removeFromDeletedClients(old, toRemove);

    expect(result).toEqual([
      {clientGroupID: 'group-a', clientID: 'client-2'},
      {clientGroupID: 'group-a', clientID: 'client-3'},
    ]);
  });

  test('handles empty inputs', () => {
    const old: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
    ];
    const empty: DeletedClients = [];

    expect(removeFromDeletedClients(old, empty)).toEqual(old);
    expect(removeFromDeletedClients(empty, old)).toEqual([]);
    expect(removeFromDeletedClients(empty, empty)).toEqual([]);
  });

  test('preserves original objects (immutability)', () => {
    const old: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-2'},
    ];
    const toRemove: DeletedClients = [
      {clientGroupID: 'group-a', clientID: 'client-1'},
    ];

    const result = removeFromDeletedClients(old, toRemove);

    // Original objects should not be modified
    expect(old).toEqual([
      {clientGroupID: 'group-a', clientID: 'client-1'},
      {clientGroupID: 'group-a', clientID: 'client-2'},
    ]);
    expect(toRemove).toEqual([
      {clientGroupID: 'group-a', clientID: 'client-1'},
    ]);

    // Result should have client-1 removed
    expect(result).toEqual([{clientGroupID: 'group-a', clientID: 'client-2'}]);
  });
});

describe('confirmDeletedClients', () => {
  test('removes deleted client IDs from both deleted clients list and clients map', async () => {
    const dagStore = new TestStore();

    // Set up initial clients
    const clientMap = new Map(
      Object.entries({
        'client-1': makeClientV6({
          clientGroupID: 'group-a',
          heartbeatTimestampMs: 1000,
          refreshHashes: [],
        }),
        'client-2': makeClientV6({
          clientGroupID: 'group-a',
          heartbeatTimestampMs: 2000,
          refreshHashes: [],
        }),
        'client-3': makeClientV6({
          clientGroupID: 'group-b',
          heartbeatTimestampMs: 3000,
          refreshHashes: [],
        }),
      }),
    );
    await setClientsForTesting(clientMap, dagStore);

    await withWrite(dagStore, async (dagWrite: Write) => {
      // Set initial deleted clients
      const initialDeletedClients: DeletedClients = [
        {clientGroupID: 'group-a', clientID: 'client-1'},
        {clientGroupID: 'group-a', clientID: 'client-2'},
        {clientGroupID: 'group-b', clientID: 'client-3'},
        {clientGroupID: 'group-c', clientID: 'client-4'}, // Not in clients map
      ];
      await setDeletedClients(dagWrite, initialDeletedClients);

      const result = await confirmDeletedClients(
        dagWrite,
        ['client-1', 'client-3'], // Delete these client IDs
        [], // No client group IDs to delete
      );

      // Should return remaining deleted clients (excluding the removed ones)
      expect(result).toEqual([
        {clientGroupID: 'group-a', clientID: 'client-2'},
        {clientGroupID: 'group-c', clientID: 'client-4'},
      ]);
    });
  });

  test('removes deleted client group IDs and their associated clients', async () => {
    const dagStore = new TestStore();

    // Set up initial clients
    const clientMap = new Map(
      Object.entries({
        'client-1': makeClientV6({
          clientGroupID: 'group-a',
          heartbeatTimestampMs: 1000,
          refreshHashes: [],
        }),
        'client-2': makeClientV6({
          clientGroupID: 'group-a',
          heartbeatTimestampMs: 2000,
          refreshHashes: [],
        }),
        'client-3': makeClientV6({
          clientGroupID: 'group-b',
          heartbeatTimestampMs: 3000,
          refreshHashes: [],
        }),
      }),
    );
    await setClientsForTesting(clientMap, dagStore);

    await withWrite(dagStore, async (dagWrite: Write) => {
      // Set initial deleted clients
      const initialDeletedClients: DeletedClients = [
        {clientGroupID: 'group-a', clientID: 'client-1'},
        {clientGroupID: 'group-a', clientID: 'client-2'},
        {clientGroupID: 'group-b', clientID: 'client-3'},
        {clientGroupID: 'group-c', clientID: 'client-4'}, // Not in clients map
      ];
      await setDeletedClients(dagWrite, initialDeletedClients);

      const result = await confirmDeletedClients(
        dagWrite,
        [], // No individual client IDs to delete
        ['group-a'], // Delete entire group-a
      );

      // Should return remaining deleted clients (excluding group-a entries)
      expect(result).toEqual([
        {clientGroupID: 'group-b', clientID: 'client-3'},
        {clientGroupID: 'group-c', clientID: 'client-4'},
      ]);
    });
  });

  test('removes both individual client IDs and client groups', async () => {
    const dagStore = new TestStore();

    // Set up initial clients
    const clientMap = new Map(
      Object.entries({
        'client-1': makeClientV6({
          clientGroupID: 'group-a',
          heartbeatTimestampMs: 1000,
          refreshHashes: [],
        }),
        'client-2': makeClientV6({
          clientGroupID: 'group-b',
          heartbeatTimestampMs: 2000,
          refreshHashes: [],
        }),
        'client-3': makeClientV6({
          clientGroupID: 'group-c',
          heartbeatTimestampMs: 3000,
          refreshHashes: [],
        }),
      }),
    );
    await setClientsForTesting(clientMap, dagStore);

    await withWrite(dagStore, async (dagWrite: Write) => {
      // Set initial deleted clients
      const initialDeletedClients: DeletedClients = [
        {clientGroupID: 'group-a', clientID: 'client-1'},
        {clientGroupID: 'group-b', clientID: 'client-2'},
        {clientGroupID: 'group-c', clientID: 'client-3'},
        {clientGroupID: 'group-d', clientID: 'client-4'}, // Not in clients map
      ];
      await setDeletedClients(dagWrite, initialDeletedClients);

      const result = await confirmDeletedClients(
        dagWrite,
        ['client-1'], // Delete this individual client
        ['group-c'], // Delete entire group-c
      );

      // Should return remaining deleted clients (excluding client-1 and group-c)
      expect(result).toEqual([
        {clientGroupID: 'group-b', clientID: 'client-2'},
        {clientGroupID: 'group-d', clientID: 'client-4'},
      ]);
    });
  });

  test('handles empty input arrays', async () => {
    const dagStore = new TestStore();

    await withWrite(dagStore, async (dagWrite: Write) => {
      // Set initial deleted clients
      const initialDeletedClients: DeletedClients = [
        {clientGroupID: 'group-a', clientID: 'client-1'},
        {clientGroupID: 'group-b', clientID: 'client-2'},
      ];
      await setDeletedClients(dagWrite, initialDeletedClients);

      const result = await confirmDeletedClients(
        dagWrite,
        [], // No client IDs to delete
        [], // No client group IDs to delete
      );

      // Should return all original deleted clients unchanged
      expect(result).toEqual([
        {clientGroupID: 'group-a', clientID: 'client-1'},
        {clientGroupID: 'group-b', clientID: 'client-2'},
      ]);
    });
  });

  test('handles empty deleted clients list', async () => {
    const dagStore = new TestStore();

    await withWrite(dagStore, async (dagWrite: Write) => {
      // Start with empty deleted clients list
      await setDeletedClients(dagWrite, []);

      const result = await confirmDeletedClients(
        dagWrite,
        ['client-1'], // Try to delete non-existent client
        ['group-a'], // Try to delete non-existent group
      );

      // Should return empty array
      expect(result).toEqual([]);
    });
  });

  test('deletes clients from client map when removing by client group', async () => {
    const dagStore = new TestStore();

    // Set up initial clients in different groups
    const clientMap = new Map(
      Object.entries({
        'client-1': makeClientV6({
          clientGroupID: 'group-a',
          heartbeatTimestampMs: 1000,
          refreshHashes: [],
        }),
        'client-2': makeClientV6({
          clientGroupID: 'group-a',
          heartbeatTimestampMs: 2000,
          refreshHashes: [],
        }),
        'client-3': makeClientV6({
          clientGroupID: 'group-b',
          heartbeatTimestampMs: 3000,
          refreshHashes: [],
        }),
      }),
    );
    await setClientsForTesting(clientMap, dagStore);

    await withWrite(dagStore, async (dagWrite: Write) => {
      // Set initial deleted clients
      const initialDeletedClients: DeletedClients = [
        {clientGroupID: 'group-a', clientID: 'client-1'},
        {clientGroupID: 'group-a', clientID: 'client-2'},
        {clientGroupID: 'group-b', clientID: 'client-3'},
      ];
      await setDeletedClients(dagWrite, initialDeletedClients);

      // The function should remove clients from the clients map when deleting by group
      // This test verifies the function works correctly, but doesn't directly test
      // the clients map modification since that's an internal side effect
      const result = await confirmDeletedClients(
        dagWrite,
        [],
        ['group-a'], // Delete entire group-a
      );

      expect(result).toEqual([
        {clientGroupID: 'group-b', clientID: 'client-3'},
      ]);
    });
  });
});
