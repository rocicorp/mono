import {getFunctions} from 'firebase/functions';
import type {BaseRequest} from 'mirror-protocol/src/base.js';

export interface TailEventSource {
  onMessage: (message: string) => void;
  startListening: () => Promise<void>;
}

export async function createEventSource<R extends BaseRequest>(
  functionName: string,
  appID: string,
  apiToken: string,
  request: R,
): Promise<TailEventSource> {
  let onMsg: ((message: string) => void) | undefined;

  const headers = {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    'Authorization': `Bearer ${apiToken}`,
    'Content-Type': 'text/event-stream',
  };

  const url = createEventSourceUrl(getFunctions(), functionName, appID);

  const response = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(request),
  });

  const reader = response.body
    ?.pipeThrough(new TextDecoderStream())
    .getReader();
  if (!reader) throw new Error('SSEReader is undefined');

  const eventSource: TailEventSource = {
    set onMessage(listener: (message: string) => void) {
      onMsg = listener;
    },
    get onMessage() {
      // eslint-disable-next-line @typescript-eslint/no-empty-function
      return onMsg || (() => {});
    },
    async startListening() {
      for (;;) {
        const {done, value} = await reader.read();
        if (done) break;
        if (onMsg && value) {
          onMsg(value);
        }
      }
    },
  };

  return eventSource;
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
