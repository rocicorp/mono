import {userAgent} from './version.js';

export type Requester = {
  requester: {
    userID: string;
    userAgent: typeof userAgent;
  };
};

export function makeRequester(userID: string): Requester {
  return {
    requester: {
      userID,
      userAgent,
    },
  };
}
