import * as querystring from 'querystring';
import * as https from 'https';
import * as os from 'os';
import {randomUUID, createHash} from 'crypto';
import {version} from '../version.js';
const interfaces = os.networkInterfaces();

export type PrimitiveTypes = string | number | boolean;

const deviceFingerprint = createHash('md5')
  .update(JSON.stringify(interfaces))
  .digest('hex');

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
export type EventNames =
  | 'cmd_login'
  | 'cmd_dev'
  | 'cmd_status'
  | 'cmd_publish'
  | 'cmd_init'
  | 'cnd_create'
  | 'cmd_tail'
  | 'cmd_delete'
  | 'cmd_create'
  | 'error';

export async function sendAnalyticsEvent(eventName: EventNames): Promise<void> {
  await sendGAEvent([
    {
      en: eventName,
    },
  ]);
}

function createRequestParameter(): string {
  const requestParameters: Partial<Record<RequestParameter, PrimitiveTypes>> = {
    [RequestParameter.ProtocolVersion]: 2,
    [RequestParameter.ClientId]: deviceFingerprint,
    [RequestParameter.UserId]: deviceFingerprint,
    [RequestParameter.TrackingId]: TRACKING_ID,
    [RequestParameter.AppVersion]: version,
    //node version
    [RequestParameter.Dimension1]: process.version,
    // Built-in user properties
    [RequestParameter.SessionId]: randomUUID(),
    [RequestParameter.UserAgentArchitecture]: os.arch(),
    [RequestParameter.UserAgentPlatform]: os.platform(),
    [RequestParameter.UserAgentPlatformVersion]: os.release(),
    [RequestParameter.UserAgentMobile]: 0,
    [RequestParameter.SessionEngaged]: 1,
    // The below is needed for tech details to be collected.
    [RequestParameter.UserAgentFullVersionList]:
      'Google%20Chrome;111.0.5563.64|Not(A%3ABrand;8.0.0.0|Chromium;111.0.5563.64',
  };

  const requestParameterStringified = querystring.stringify(requestParameters);
  return requestParameterStringified;
}

function sendGAEvent(data: Record<string, PrimitiveTypes | undefined>[]) {
  return new Promise<void>((resolve, reject) => {
    const request = https.request(
      {
        host: 'www.google-analytics.com',
        method: 'POST',
        path: '/g/collect?' + createRequestParameter(),
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

    console.log('request', request);
    request.on('error', reject);
    const queryParameters = data.map(p => querystring.stringify(p)).join('\n');
    console.log('queryParams', queryParameters);
    request.write(queryParameters);
    request.end();
  });
}
