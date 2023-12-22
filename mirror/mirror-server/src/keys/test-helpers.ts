import {jest} from '@jest/globals';

export function mockKeyUpdater() {
  return {
    call: jest.fn().mockImplementation(() => Promise.resolve({})),
  };
}
