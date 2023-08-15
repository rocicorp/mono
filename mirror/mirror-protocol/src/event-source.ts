import EventSource from 'eventsource';
import {getFunctions} from 'firebase/functions';

export function createEventSource(
  functionName: string,
  appID: string,
  apiToken: string,
): EventSource {
  const headers = {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    Authorization: `Bearer ${apiToken}`,
  };
  const url = createEventSourceUrl(getFunctions(), functionName, appID);
  return new EventSource(url, {
    headers,
  });
}

function createEventSourceUrl(
  functions: ReturnType<typeof getFunctions> & {
    emulatorOrigin?: string;
  },
  functionName: string,
  appID: string,
): string {
  if (functions.emulatorOrigin) {
    return `${functions.emulatorOrigin}/${functions.app.options.projectId}/${functions.region}/${functionName}/${appID}`;
  }
  return `https://${functions.region}-${functions.app.options.projectId}.cloudfunctions.net/${functionName}/${appID}`;
}
