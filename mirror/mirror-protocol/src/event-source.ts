import EventSource from 'eventsource';

export function createEventSource(
  functionName: string,
  appID: string,
  apiToken: string,
): EventSource {
  const headers = {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    Authorization: `Bearer ${apiToken}`,
  };
  return new EventSource(
    //todo:cesar need to change this based on configuration
    `http://127.0.0.1:5001/reflect-mirror-staging/us-central1/${functionName}/${appID}`,
    {
      headers,
    },
  );
}
