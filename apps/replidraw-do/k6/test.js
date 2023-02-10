import ws from "k6/ws";
import { check } from "k6";
import { Trend } from "k6/metrics";

const numShapesPerClient = __ENV.SHAPES_PER_CLIENT || 1;
const roomID = __ENV.ROOM_ID;

// e.g.: wss://replidraw.replicache.workers.dev/connect or ws://[::1]:8787/connect
// Note: it looks like wrangler only listens on ipv6!
// https://github.com/cloudflare/wrangler/issues/1198#issuecomment-1204690449
const socketBaseURL = __ENV.SOCKET_BASE_URL || "ws://[::1]:8787/connect";

if (!roomID) {
  throw new Error("Must specify a ROOM_ID env variable");
}

function randomID() {
  return Math.random().toString(36).substring(2);
}

const sentMutations = [];
const receivedPokes = [];
const pokeWaitTrend = new Trend("poke_wait_time");

function sendMutation(socket, clientID, name, args) {
  const lastMutation = sentMutations[sentMutations.length - 1];
  const lmid = lastMutation ? lastMutation.id : 0;
  const id = lmid + 1;
  const ts = Date.now();
  const mutation = {
    id,
    name,
    args,
    timestamp: ts,
  };
  const pushBody = {
    clientID,
    mutations: [mutation],
    pushVersion: 1,
    schemaVersion: "",
    timestamp: ts,
  };
  sentMutations.push({ id, ts });
  const msg = JSON.stringify(["push", pushBody]);
  console.info("sending", msg);
  socket.send(msg);
}

function createShape(socket, clientID, idx) {
  sendMutation(socket, clientID, "createShape", {
    id: `${clientID}-${idx}`,
    shape: randomShape(),
  });
}

function scanShape(socket, clientID, idx) {
  sendMutation(socket, clientID, "scanShape", {
    id: `${clientID}-${idx}`,
    dx: 1,
    maxX: 500,
  });
}

function createShapes(socket, clientID, numShapes) {
  for (let i = 0; i < numShapes; i++) {
    createShape(socket, clientID, i);
  }
}

function scanShapes(socket, clientID, numShapes) {
  for (let i = 0; i < numShapes; i++) {
    scanShape(socket, clientID, i);
  }
}

export default function () {
  const clientID = randomID();
  const userID = randomID();

  const url = `${socketBaseURL}?clientID=${clientID}&roomID=${roomID}&baseCookie=0&lmid=0&ts=${Date.now()}`;
  const params = {
    headers: {
      "Sec-WebSocket-Protocol": encodeURIComponent(
        JSON.stringify({
          roomID,
          userID,
        })
      ),
    },
  };

  const response = ws.connect(url, params, function (socket) {
    socket.on("open", function open() {
      console.log("opened");
    });

    socket.on("message", function (message) {
      console.log(`Received message: ${message}`);
      const [type, body] = JSON.parse(message);
      if (type === "connected") {
        createShapes(socket, clientID, numShapesPerClient);
        socket.setInterval(() => {
          scanShapes(socket, clientID, numShapesPerClient);
        }, 16);
      } else {
        const lastPoke = receivedPokes[receivedPokes.length - 1];
        const lastLMID = lastPoke ? lastPoke.id : -1;
        const id = body.lastMutationID;
        const ts = Date.now();
        for (const m of sentMutations.reverse()) {
          if (m.id <= id) {
            pokeWaitTrend.add(ts - m.ts);
            break;
          }
        }
        check(type, {
          "message type was poke": (res) => res === "poke",
        });
        check(body, {
          "body is an object": (res) => typeof res === "object",
          "received correct lmid": (res) => id >= lastLMID + 1,
        });
        receivedPokes.push({ id, ts: Date.now() });
      }
    });

    socket.on("close", function () {
      console.log("disconnected");
    });

    socket.on("error", function (e) {
      if (e.error() != "websocket: close sent") {
        console.log("An unexpected error occured: ", e.error());
      }
    });

    socket.setTimeout(function () {
      console.log("test done, closing the socket");
      socket.close();
    }, 5000);
  });

  check(response, { "status is 101": (r) => r && r.status === 101 });
}

export function randomShape() {
  const colors = ["red", "blue", "white", "green", "yellow"];
  const s = randInt(100, 400);
  const fill = colors[randInt(0, colors.length - 1)];
  return {
    type: "rect",
    x: randInt(0, 400),
    y: randInt(0, 400),
    width: s,
    height: s,
    rotate: randInt(0, 359),
    fill,
  };
}

export function randInt(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min + 1) + min); //The maximum is inclusive and the minimum is inclusive
}
