import * as querystring from 'querystring';
import * as https from 'https';
import * as os from 'os';
import {randomUUID, createHash} from 'crypto';
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
  DebugView = '_dbg',
  GtmVersion = 'gtm',
  Language = 'ul',
  NewToSite = '_nsi',
  NonInteraction = 'ni',
  PageLocation = 'dl',
  PageTitle = 'dt',
  ProtocolVersion = 'v',
  SessionEngaged = 'seg',
  SessionId = 'sid',
  SessionNumber = 'sct',
  SessionStart = '_ss',
  TrackingId = 'tid',
  TrafficType = 'tt',
  UserAgentArchitecture = 'uaa',
  UserAgentBitness = 'uab',
  UserAgentFullVersionList = 'uafvl',
  UserAgentMobile = 'uamb',
  UserAgentModel = 'uam',
  UserAgentPlatform = 'uap',
  UserAgentPlatformVersion = 'uapv',
  UserId = 'uid',
}

export async function sendEvent(
  eventName: string,
  parameters?: Record<string, PrimitiveTypes>,
): Promise<void> {
  const params = {
    nodeVersion: process.version,
    eventName,
    ...parameters,
  };
  await sendGAEvent([
    {
      ...params,
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
