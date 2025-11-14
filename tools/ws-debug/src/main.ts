import WebSocket from 'ws';
import {consoleLogSink, LogContext} from '@rocicorp/logger';

// The WebSocket connection details from the fetch request
const WS_URL =
  'ws://localhost:4848/sync/v40/connect?clientID=ku7aode2vofomjp8g9&clientGroupID=ao5g82gvq6t033g3v6&userID=LlcOS6u-Dv5xBIihbE9UC&baseCookie=64oznwo8%3A06&ts=58467.700000047684&lmid=0&wsid=P-OGZHKQxgKT8NKlTdBtl';

const SEC_WEBSOCKET_PROTOCOL =
  'eyJpbml0Q29ubmVjdGlvbk1lc3NhZ2UiOlsiaW5pdENvbm5lY3Rpb24iLHsiZGVzaXJlZFF1ZXJpZXNQYXRjaCI6W10sInVzZXJQdXNoVVJMIjoiaHR0cDovL2xvY2FsaG9zdDo1MTczL2FwaS9tdXRhdGUiLCJ1c2VyUXVlcnlVUkwiOiJodHRwOi8vbG9jYWxob3N0OjUxNzMvYXBpL2dldC1xdWVyaWVzIiwiYWN0aXZlQ2xpZW50cyI6WyJrdTdhb2RlMXZvZm9tanA4ZzkiXX1dLCJhdXRoVG9rZW4iOiJleUpoYkdjaU9pSlFVekkxTmlKOS5leUp6ZFdJaU9pSk1iR05QVXpaMUxVUjJOWGhDU1dsb1lrVTVWVU1pTENKcFlYUWlPakUzTmpNeE16QTRNRGdzSW5KdmJHVWlPaUpqY21WM0lpd2libUZ0WlNJNkluUmhiblJoYldGdUlpd2laWGh3SWpveE56WTFOekl5T0RBNGZRLnZZRkcyZWtPZk1kR2VQQmw3VHVmVndQbC1fSjJiaUk0YjJzbXBjbmdkdXNQRUZCS3I3Q1JlZ252dVVYaW9fNjhfLXdZcUI3WFd6RzBUUGNHcUhsWTk3WXE3YVBqZ0U5SVVxZ3NLLVFxdU9PamV2WUZ6MWNOMDhCdkZLcGVNdm80Qk83d2VELXZZblR0VHlTTTFSOVVTRXpDLTNRVkc4LUM4cU9vTzczTnFWeGpadlUtZUpFWk5FamJZWHJITnJ6azlvandLeGNMMHVOaTZIUkZFTnNpUXJDT0F3ZlpTTm9zTU5RUkpQYWlsUzhOWmxPVUNXZVFfTk1jRlVSd0RYQ0JibnJjZUYtMXpoVGxvdmsyV0UwcVEtcXNCcTZMQlVKTHRNdWF2ZkE3b1pzQWVpMU9TSTVnZ25iTkZUdndNTHI2b0VXMjAyeWtBVllXQXJQWmhsRkZHdyJ9';

const CAPTURE_DURATION_MS = 5000; // 5 seconds

const lc = new LogContext('debug', {}, consoleLogSink);

interface ConnectionMetadata {
  url: string;
  clientID: string;
  clientGroupID: string;
  userID: string;
  baseCookie: string;
  timestamp: string;
  lmid: string;
  wsid: string;
  protocol: {
    initConnectionMessage?: unknown;
    authToken?: string;
  };
}

function parseUrl(url: string): {
  url: string;
  clientID: string | null;
  clientGroupID: string | null;
  userID: string | null;
  baseCookie: string | null;
  timestamp: string | null;
  lmid: string | null;
  wsid: string | null;
} {
  const urlObj = new URL(url);
  const params = new URLSearchParams(urlObj.search);

  return {
    url,
    clientID: params.get('clientID'),
    clientGroupID: params.get('clientGroupID'),
    userID: params.get('userID'),
    baseCookie: params.get('baseCookie'),
    timestamp: params.get('ts'),
    lmid: params.get('lmid'),
    wsid: params.get('wsid'),
  };
}

function decodeProtocol(encoded: string): {
  initConnectionMessage?: unknown;
  authToken?: string;
} {
  try {
    // Decode: URI decode -> base64 decode -> UTF-8 decode -> JSON parse
    const uriDecoded = decodeURIComponent(encoded);
    const base64Decoded = atob(uriDecoded);
    // Convert binary string to UTF-8
    const utf8Bytes = new Uint8Array(base64Decoded.length);
    for (let i = 0; i < base64Decoded.length; i++) {
      utf8Bytes[i] = base64Decoded.charCodeAt(i);
    }
    const utf8String = new TextDecoder().decode(utf8Bytes);
    return JSON.parse(utf8String) as {
      initConnectionMessage?: unknown;
      authToken?: string;
    };
  } catch (e) {
    lc.error?.('Failed to decode protocol header:', e);
    return {};
  }
}

function printConnectionMetadata(metadata: ConnectionMetadata): void {
  console.log('\n' + '='.repeat(80));
  console.log('CONNECTION METADATA');
  console.log('='.repeat(80));
  console.log(`URL:           ${metadata.url}`);
  console.log(`Client ID:     ${metadata.clientID}`);
  console.log(`Client Group:  ${metadata.clientGroupID}`);
  console.log(`User ID:       ${metadata.userID}`);
  console.log(`Base Cookie:   ${metadata.baseCookie}`);
  console.log(`Timestamp:     ${metadata.timestamp}`);
  console.log(`LMID:          ${metadata.lmid}`);
  console.log(`WSID:          ${metadata.wsid}`);
  console.log('\nPROTOCOL HEADER:');
  if (metadata.protocol.authToken) {
    // Show first 50 chars of auth token
    const tokenPreview =
      metadata.protocol.authToken.substring(0, 50) +
      '...[' +
      (metadata.protocol.authToken.length - 50) +
      ' more chars]';
    console.log(`Auth Token:    ${tokenPreview}`);
  }
  if (metadata.protocol.initConnectionMessage) {
    console.log(
      'Init Message:  ',
      JSON.stringify(metadata.protocol.initConnectionMessage, null, 2),
    );
  }
  console.log('='.repeat(80));
  console.log('\n');
}

function formatMessage(
  message: unknown,
  timestamp: number,
  startTime: number,
): void {
  const elapsed = ((timestamp - startTime) / 1000).toFixed(3);
  const timeStr = `[${elapsed}s]`;

  try {
    const parsed = JSON.parse(message as string);

    if (Array.isArray(parsed) && parsed.length >= 2) {
      const [type, body] = parsed;
      console.log(`${timeStr} \x1b[36m${type}\x1b[0m`); // Cyan color for type
      if (body && typeof body === 'object') {
        console.log(JSON.stringify(body, null, 2));
      } else {
        console.log(body);
      }
    } else {
      console.log(`${timeStr} [unparsed]`);
      console.log(JSON.stringify(parsed, null, 2));
    }
  } catch (_e) {
    console.log(`${timeStr} [invalid JSON]`);
    console.log(message);
  }
  console.log(''); // Empty line between messages
}

async function main(): Promise<void> {
  // Parse connection metadata
  const urlMetadata = parseUrl(WS_URL);
  const protocolData = decodeProtocol(SEC_WEBSOCKET_PROTOCOL);

  const metadata: ConnectionMetadata = {
    url: urlMetadata.url ?? '',
    clientID: urlMetadata.clientID ?? 'unknown',
    clientGroupID: urlMetadata.clientGroupID ?? 'unknown',
    userID: urlMetadata.userID ?? 'unknown',
    baseCookie: urlMetadata.baseCookie ?? '',
    timestamp: urlMetadata.timestamp ?? '',
    lmid: urlMetadata.lmid ?? '',
    wsid: urlMetadata.wsid ?? '',
    protocol: protocolData,
  };

  printConnectionMetadata(metadata);

  console.log('='.repeat(80));
  console.log('MESSAGES');
  console.log('='.repeat(80));
  console.log('\n');

  const startTime = Date.now();
  let messageCount = 0;

  const ws = new WebSocket(WS_URL, {
    headers: {
      'accept-language': 'en-US,en;q=0.9',
      'cache-control': 'no-cache',
      'pragma': 'no-cache',
    },
    protocol: SEC_WEBSOCKET_PROTOCOL,
  });

  ws.on('open', () => {
    lc.info?.('WebSocket connection opened');
  });

  ws.on('message', (data: WebSocket.RawData) => {
    messageCount++;
    const timestamp = Date.now();
    // Convert WebSocket.RawData (Buffer | ArrayBuffer | Buffer[]) to string
    const message =
      data instanceof Buffer
        ? data.toString('utf8')
        : Array.isArray(data)
          ? Buffer.concat(data).toString('utf8')
          : new TextDecoder().decode(data);
    formatMessage(message, timestamp, startTime);
  });

  ws.on('error', (error: Error) => {
    console.error('\n[ERROR]', error.message);
  });

  ws.on('close', (code: number, reason: Buffer) => {
    const reasonStr = reason.toString();
    console.log(
      `\n[CLOSED] Code: ${code}, Reason: ${reasonStr || '(no reason)'}`,
    );
  });

  // Wait for the capture duration
  await new Promise(resolve => setTimeout(resolve, CAPTURE_DURATION_MS));

  // Close the connection
  ws.close();

  // Wait a bit for the close event to fire
  await new Promise(resolve => setTimeout(resolve, 500));

  console.log('\n' + '='.repeat(80));
  console.log(
    `SUMMARY: Captured ${messageCount} messages in ${CAPTURE_DURATION_MS / 1000}s`,
  );
  console.log('='.repeat(80));
}

main().catch(e => {
  lc.error?.('Fatal error:', e);
  process.exit(1);
});
