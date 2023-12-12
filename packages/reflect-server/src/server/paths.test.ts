import {describe, expect, test} from '@jest/globals';
import {
  CREATE_ROOM_PATH,
  DELETE_ROOM_PATH,
  INVALIDATE_ALL_CONNECTIONS_PATH,
  INVALIDATE_USER_CONNECTIONS_PATH,
  fmtPath,
} from './paths.js';

describe('paths', () => {
  type Case = {
    name: string;
    pattern: string;
    groups?: Record<string, string>;
    result: string;
  };
  const cases: Case[] = [
    {
      name: 'simple roomID',
      pattern: CREATE_ROOM_PATH,
      groups: {roomID: 'foo-room'},
      result: '/api/v1/rooms/foo-room:create',
    },
    {
      name: 'roomID with special characters',
      pattern: DELETE_ROOM_PATH,
      groups: {roomID: 'room/id?with\\special:characters'},
      result: '/api/v1/rooms/room%2Fid%3Fwith%5Cspecial%3Acharacters:delete',
    },
    {
      name: 'userID',
      pattern: INVALIDATE_USER_CONNECTIONS_PATH,
      groups: {userID: 'foo-user'},
      result: '/api/v1/connections/users/foo-user:invalidate',
    },
    {
      name: 'no groups',
      pattern: INVALIDATE_ALL_CONNECTIONS_PATH,
      result: '/api/v1/connections:invalidate',
    },
  ];

  cases.forEach(c => {
    test(c.name, () => {
      expect(fmtPath(c.pattern, c.groups)).toBe(c.result);
      const parsed = new URLPattern({pathname: c.pattern}).exec(
        `http://api.reflect-server.net${c.result}`,
      );
      const decoded = Object.fromEntries(
        Object.entries(parsed?.pathname.groups ?? {}).map(([name, value]) => [
          name,
          decodeURIComponent(value),
        ]),
      );
      expect(decoded).toEqual(c.groups ?? {});
    });
  });
});
