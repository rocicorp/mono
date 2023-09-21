import WebSocket from 'ws';

const roomID = 'my-room';

const ws = new WebSocket(
  // 'ws://127.0.0.1:8787/api/debug/v0/puzzle-b-0000000/tail',
  // 'define-your-api-key',
  // 'wss://preview.reflect-server.net/api/debug/v0/puzzle-b-0000000/tail',
  // 'p41ntf1ght',
  `wss://arv-test-tail2-arv.reflect-server.dev/api/debug/v0/tail?roomID=${roomID}`,
  'dummy-api-key',
);
ws.onerror = e => {
  console.log(e.type, e.message);
};

ws.onmessage = e => {
  console.log(e.data);
};

ws.onclose = e => {
  console.log(e.code, e.reason);
};

ws.onopen = e => {
  console.log(e.type);
};
