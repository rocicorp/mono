import {getUserAgent} from './version.js';
import type {UserAgent} from '../../mirror-protocol/src/user-agent.js';

export type Requester = {
  userID: string;
  userAgent: UserAgent;
};

export function makeRequester(userID: string): Requester {
  return {
    userID,
    userAgent: getUserAgent(),
  };
}
