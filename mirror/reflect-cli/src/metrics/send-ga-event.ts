import {arch, platform, release} from 'os';
import {randomUUID} from 'crypto';
import {version} from '../version.js';
import {stringify} from 'querystring';
import {
  deviceFingerprint,
  UserParameters,
  UserCustomDimension,
} from 'mirror-protocol/src/reporting.js';
import type {AuthenticatedUser} from '../auth-config.js';
const TRACKING_ID = 'G-69B1QV88XF';

/**
 * GA built-in request parameters
 */
export enum RequestParameter {
  ClientId = 'cid',
  ProtocolVersion = 'v',
  SessionEngaged = 'seg',
  SessionId = 'sid',
  TrackingId = 'tid',
  UserAgentArchitecture = 'uaa',
  UserAgentFullVersionList = 'uafvl',
  UserAgentMobile = 'uamb',
  UserAgentPlatform = 'uap',
  UserAgentPlatformVersion = 'uapv',
  UserId = 'uid',
  AppVersion = 'av',
  Dimension1 = 'cd1',
  Dimension2 = 'cd2',
}

export async function sendAnalyticsEvent(
  eventName: string,
  user: AuthenticatedUser,
): Promise<void> {
  const userParameters = getUserParameters(version);
  await sendGAEvent(
    [
      {
        ...userParameters,
        en: eventName,
      },
    ],
    user,
  );
}

function getRequestParameters(user: AuthenticatedUser): string {
  const params = {
    [RequestParameter.ProtocolVersion]: 2,
    [RequestParameter.ClientId]: deviceFingerprint,
    [RequestParameter.UserId]: user.userID,
    [RequestParameter.TrackingId]: TRACKING_ID,
    [RequestParameter.AppVersion]: version,
    [RequestParameter.Dimension1]: process.version,
    [RequestParameter.SessionId]: randomUUID(),
    [RequestParameter.UserAgentArchitecture]: arch(),
    [RequestParameter.UserAgentPlatform]: platform(),
    [RequestParameter.UserAgentPlatformVersion]: release(),
    [RequestParameter.UserAgentMobile]: 0,
    [RequestParameter.SessionEngaged]: 1,
    [RequestParameter.UserAgentFullVersionList]:
      'Google%20Chrome;111.0.5563.64|Not(A%3ABrand;8.0.0.0|Chromium;111.0.5563.64',
  };
  return stringify(params);
}

export function getUserParameters(version: string): UserParameters {
  return {
    [UserCustomDimension.OsArchitecture]: arch(),
    [UserCustomDimension.NodeVersion]: process.version,
    [UserCustomDimension.ReflectCLIVersion]: version,
    [UserCustomDimension.DeviceFingerprint]: deviceFingerprint,
  };
}

export function sendGAEvent(
  data: Record<string, string>[],
  user: AuthenticatedUser,
): Promise<void> {
  const baseUrl = 'https://www.google-analytics.com/g/collect?';
  const queryString = getRequestParameters(user);
  const fullUrl = baseUrl + queryString;

  const postBody = data.map(p => stringify(p)).join('\n');

  return fetch(fullUrl, {
    method: 'POST',
    headers: {
      'user-agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    },
    body: postBody,
  }).then(response => {
    if (!response.ok) {
      throw new Error(
        `Analytics reporting failed with status code: ${response.status}.`,
      );
    }
  });
}
