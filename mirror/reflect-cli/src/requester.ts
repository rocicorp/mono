import {userAgent} from './version.js';

export type Requester = {
  userID: string;
  userAgent: typeof userAgent;
};

export function makeRequester(userID: string): Requester {
  return {
    userID,
    userAgent,
  };
}
