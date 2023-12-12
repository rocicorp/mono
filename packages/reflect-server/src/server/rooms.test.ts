import {describe, expect, test} from '@jest/globals';
import {
  CLOSE_ROOM_PATH,
  CREATE_ROOM_PATH,
  DELETE_ROOM_PATH,
  fmtPath,
} from './paths.js';

describe('rooms', () => {
  test('makeCreateRoomPath', () => {
    expect(fmtPath(CREATE_ROOM_PATH, {roomID: 'foo-bar'})).toBe(
      '/api/v1/rooms/foo-bar:create',
    );
    expect(
      fmtPath(CREATE_ROOM_PATH, {roomID: 'id\\with/slashes-and:colons'}),
    ).toBe('/api/v1/rooms/id%5Cwith%2Fslashes-and%3Acolons:create');
    expect(fmtPath(CLOSE_ROOM_PATH, {roomID: 'foo-bar'})).toBe(
      '/api/v1/rooms/foo-bar:close',
    );
    expect(
      fmtPath(DELETE_ROOM_PATH, {roomID: 'id\\with/slashes-and:colons'}),
    ).toBe('/api/v1/rooms/id%5Cwith%2Fslashes-and%3Acolons:delete');
  });
});
