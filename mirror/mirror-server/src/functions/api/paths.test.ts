import {describe, expect, test} from '@jest/globals';
import {makeWorkerPath, parseReadParams, parseWriteParams} from './paths.js';

describe('api-paths', () => {
  type Case = {
    path: string;
    params: Record<string, string>;
    workerPath: string;
  };
  const readCases: Case[] = [
    {
      path: '/v1/apps/lm3idfw/rooms',
      params: {
        appID: 'lm3idfw',
        resource: 'rooms',
      },
      workerPath: '/api/v1/rooms',
    },
    {
      path: '/v1/apps/lm3idfw/rooms/foo-bar-id',
      params: {
        appID: 'lm3idfw',
        resource: 'rooms',
        subpath: 'foo-bar-id',
      },
      workerPath: '/api/v1/rooms/foo-bar-id',
    },
    {
      path: '/v1/apps/lm3idfw/rooms/id%2Fwith%2Fslashes/data/abc-key',
      params: {
        appID: 'lm3idfw',
        resource: 'rooms',
        subpath: 'id%2Fwith%2Fslashes/data/abc-key',
      },
      workerPath: '/api/v1/rooms/id%2Fwith%2Fslashes/data/abc-key',
    },
    {
      path: '/v1/apps/lm3idfw/connections/rooms/id%2Fwith%2Fslashes',
      params: {
        appID: 'lm3idfw',
        resource: 'connections',
        subpath: 'rooms/id%2Fwith%2Fslashes',
      },
      workerPath: '/api/v1/connections/rooms/id%2Fwith%2Fslashes',
    },
  ];

  readCases.forEach(c => {
    test(`GET ${c.path}`, () => {
      // Sanity check the test data.
      expect(
        c.path.indexOf(c.workerPath.substring('/api/v1'.length)),
      ).toBeGreaterThan(0);

      const params = parseReadParams(c.path);
      expect(params).toEqual(c.params);

      expect(makeWorkerPath(params)).toBe(c.workerPath);
    });
  });

  const writeCases: Case[] = [
    {
      path: '/v1/apps/lm3idfw/rooms:foo',
      params: {
        appID: 'lm3idfw',
        resource: 'rooms',
        command: 'foo',
      },
      workerPath: '/api/v1/rooms:foo',
    },
    {
      path: '/v1/apps/lm3idfw/rooms/foo-bar-id:close',
      params: {
        appID: 'lm3idfw',
        resource: 'rooms',
        subpath: 'foo-bar-id',
        command: 'close',
      },
      workerPath: '/api/v1/rooms/foo-bar-id:close',
    },
    {
      path: '/v1/apps/lm3idfw/rooms/id%2Fwith%2Fslashes/data/abc-key:delete',
      params: {
        appID: 'lm3idfw',
        resource: 'rooms',
        subpath: 'id%2Fwith%2Fslashes/data/abc-key',
        command: 'delete',
      },
      workerPath: '/api/v1/rooms/id%2Fwith%2Fslashes/data/abc-key:delete',
    },
    {
      path: '/v1/apps/lm3idfw/connections:invalidate',
      params: {
        appID: 'lm3idfw',
        resource: 'connections',
        command: 'invalidate',
      },
      workerPath: '/api/v1/connections:invalidate',
    },
    {
      path: '/v1/apps/lm3idfw/connections/all:invalidate',
      params: {
        appID: 'lm3idfw',
        resource: 'connections',
        subpath: 'all',
        command: 'invalidate',
      },
      workerPath: '/api/v1/connections/all:invalidate',
    },
    {
      path: '/v1/apps/lm3idfw/connections/rooms/id%2Fwith%2Fslashes:invalidate',
      params: {
        appID: 'lm3idfw',
        resource: 'connections',
        subpath: 'rooms/id%2Fwith%2Fslashes',
        command: 'invalidate',
      },
      workerPath: '/api/v1/connections/rooms/id%2Fwith%2Fslashes:invalidate',
    },
  ];

  writeCases.forEach(c => {
    test(`POST ${c.path}`, () => {
      // Sanity check for test data.
      expect(
        c.path.indexOf(c.workerPath.substring('/api/v1'.length)),
      ).toBeGreaterThan(0);

      const params = parseWriteParams(c.path);
      expect(params).toEqual(c.params);

      expect(makeWorkerPath(params)).toBe(c.workerPath);
    });
  });
});
