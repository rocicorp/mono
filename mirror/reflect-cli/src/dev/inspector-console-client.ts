import {resolver} from '@rocicorp/resolver';
import type {Protocol} from 'devtools-protocol';
import WebSocket from 'ws';
import {logConsoleMessage} from './log-console-message.js';

// https://chromedevtools.github.io/devtools-protocol/#endpoints
interface InspectorWebSocketTarget {
  id: string;
  title: string;
  type: 'node';
  description: string;
  webSocketDebuggerUrl: string;
  devtoolsFrontendUrl: string;
  devtoolsFrontendUrlCompat: string;
  faviconUrl: string;
  url: string;
}

export async function inspectorConsoleClient(
  url: URL,
  inspectorPort: number,
  signal: AbortSignal,
) {
  const wsURL = await findWebSocketDebuggerURL(url, inspectorPort);
  if (wsURL) {
    await listenToConsoleAPI(wsURL, signal);
  }
}

async function findWebSocketDebuggerURL(
  url: URL,
  inspectorPort: number,
): Promise<string | undefined> {
  const u2 = new URL(url);
  u2.port = String(inspectorPort);
  u2.pathname = '/json/list';

  try {
    const res = await fetch(u2);
    const targets = (await res.json()) as InspectorWebSocketTarget[];

    return targets.find(t => t.id.startsWith('core:user'))
      ?.webSocketDebuggerUrl;
  } catch (e) {
    console.error('Failed to connect to inspector', e);
  }
  return undefined;
}

async function listenToConsoleAPI(url: string, signal: AbortSignal) {
  let id = 1;
  function send(method: string) {
    ws.send(JSON.stringify({id: id++, method}));
  }

  const ws = new WebSocket(url);
  signal.addEventListener(
    'abort',
    () => {
      send('Runtime.disable');
      ws.close();
    },
    {once: true},
  );

  const openResolver = resolver<void>();
  ws.onopen = () => openResolver.resolve();
  ws.onerror = e => openResolver.reject(e);
  await openResolver.promise;

  send('Runtime.enable');

  ws.onmessage = e => {
    const {data} = e;
    if (typeof data === 'string') {
      const message = JSON.parse(data);

      switch (message.method) {
        case 'Runtime.consoleAPICalled':
          logConsoleMessage(
            message.params as Protocol.Runtime.ConsoleAPICalledEvent,
          );
          break;
        case 'Runtime.exceptionThrown': {
          // TODO(arv): Sourcemaps
          // TODO(arv): Implement this
          const params =
            message.params as Protocol.Runtime.ExceptionThrownEvent;
          console.error(
            params.exceptionDetails.exception?.preview?.description,
          );
          break;
        }
      }
    } else {
      console.log('unexpected message from devtools', data);
    }
  };
}
