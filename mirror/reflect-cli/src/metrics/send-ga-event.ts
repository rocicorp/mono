import * as querystring from 'querystring';
import * as https from 'https';
import {networkInterfaces, arch, platform, release} from 'os';
import {randomUUID, createHash} from 'crypto';
import {version} from '../version.js';
import {stringify} from 'querystring';

const TRACKING_ID = 'G-69B1QV88XF';

export type PrimitiveTypes = string | number | boolean;

const deviceFingerprint = computeFingerprint();

function computeFingerprint(): string {
  return createHash('md5')
    .update(JSON.stringify(networkInterfaces()))
    .digest('hex');
}

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

export enum UserCustomDimension {
  OsArchitecture = 'up.reflect_os_architecture',
  NodeVersion = 'up.reflect_node_version',
  ReflectCLIVersion = 'up.reflect_cli_version',
}

export async function sendAnalyticsEvent(eventName: string): Promise<void> {
  const userParameters = {
    [UserCustomDimension.OsArchitecture]: arch(),
    [UserCustomDimension.NodeVersion]: process.version,
    [UserCustomDimension.ReflectCLIVersion]: version,
  };

  await sendGAEvent([
    {
      ...userParameters,
      en: eventName,
    },
  ]);
}

function getRequestParameters(): string {
  const params = {
    [RequestParameter.ProtocolVersion]: 2,
    [RequestParameter.ClientId]: deviceFingerprint,
    [RequestParameter.UserId]: deviceFingerprint,
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

function sendGAEvent(data: Record<string, PrimitiveTypes | undefined>[]) {
  return new Promise<void>((resolve, reject) => {
    const request = https.request(
      {
        host: 'www.google-analytics.com',
        method: 'POST',
        path: '/g/collect?' + getRequestParameters(),
        headers: {
          // The below is needed for tech details to be collected even though we provide our own information from the OS Node.js module
          'user-agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
        },
      },
      response => {
        // The below is needed as otherwise the response will never close which will cause the CLI not to terminate.
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        response.on('data', () => {});

        if (response.statusCode !== 200 && response.statusCode !== 204) {
          reject(
            new Error(
              `Analytics reporting failed with status code: ${response.statusCode}.`,
            ),
          );
        } else {
          resolve();
        }
      },
    );
    request.on('error', reject);
    const queryParameters = data.map(p => querystring.stringify(p)).join('\n');
    request.write(queryParameters);
    request.end();
  });
}
