import ws from "k6/ws";
import { check } from "k6";

const numShapesPerClient = __ENV.SHAPES_PER_CLIENT || 1;
const roomID = __ENV.ROOM_ID;

if (!roomID) {
  throw new Error("Must specify a ROOM_ID env variable");
}

function randomID() {
  return Math.random().toString(36).substring(2);
}

let lastMutationID = 0;

function sendMutation(socket, name, args) {
  const mutation = {
    id: ++lastMutationID,
    name,
    args,
    timestamp: Date.now(),
  };
  const pushBody = {
    mutations: [mutation],
    pushVersion: 1,
    schemaVersion: "",
    timestamp: Date.now(),
  };
  const msg = JSON.stringify(["push", pushBody]);
  console.info("sending", msg);
  socket.send(msg);
}

function createShape(socket, clientID, idx) {
  sendMutation(socket, "createShape", {
    id: `${clientID}-${idx}`,
    shape: randomShape(),
  });
}

function scanShape(socket, clientID, idx) {
  sendMutation(socket, "scanShape", {
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

  const socketBaseURL = "ws://127.0.0.1:8787/connect";
  const url = `${socketBaseURL}?clientID=${clientID}&roomID=${roomID}&baseCookie=0&ts=${Date.now()}`;
  const params = {
    headers: {
      "Sec-WebSocket-Protocol": JSON.stringify({
        roomID,
        userID,
      }),
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
      console.log("2 seconds passed, closing the socket");
      socket.close();
    }, 30000);
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
