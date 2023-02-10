test("foo", () => {
  console.log("hi");
});

/*
import { ClientID, ClientMap, Socket } from "../../src/types/client-state";
import { Mocket } from "../util/test-utils";
import { sleep } from "../../src/util/sleep";
import { Server } from "../../src/server/server";
import { MessageHandler, CloseHandler } from "../../src/server/connect";
test("serialization", async () => {
  const s1 = new Mocket();
  const url = "u1";
  const clients: ClientMap = new Map();
  const roomID = "r1";
  const clientID = "c1";
  const data = "data";

  const log: string[] = [];

  const messageHandler = (
    pClients: ClientMap,
    pClientID: ClientID,
    pData: string,
    pWS: Socket
  ) => {
    log.push("> message");
    expect(pClients).toEqual(clients);
    expect(pClientID).toEqual(clientID);
    expect(pData).toEqual(data);
    expect(pWS).toEqual(s1);
    log.push("< message");
  };

  const closeHandler = (
    pRooms: RoomMap,
    pRoomID: RoomID,
    pClientID: ClientID
  ) => {
    log.push("> close");
    expect(pRooms).toEqual(rooms);
    expect(pRoomID).toEqual(roomID);
    expect(pClientID).toEqual(clientID);
    log.push("< close");
  };

  const connectHandler = async (
    pWS: Socket,
    pURL: string,
    pRooms: RoomMap,
    onMessage: MessageHandler,
    onClose: CloseHandler
  ): Promise<void> => {
    expect(pWS).toEqual(s1);
    expect(pURL).toEqual(url);
    expect(pRooms).deep.toEqual(rooms);
    log.push("> connect");
    onMessage(roomID, clientID, data, pWS);
    onClose(roomID, clientID);
    await sleep(10);
    onMessage(roomID, clientID, data, pWS);
    onClose(roomID, clientID);
    log.push("< connect");
  };

  const server = new Server(
    rooms,
    () => {},
    () => 42,
    () => {}
  );
  server.handleConnection(s1, url);
  server.handleConnection(s1, url);
  await sleep(50);
  expect(log).deep.toEqual([
    "> connect",
    "< connect",
    "> connect",
    "< connect",
    "> message",
    "< message",
    "> close",
    "< close",
    "> message",
    "< message",
    "> close",
    "< close",
    "> message",
    "< message",
    "> close",
    "< close",
    "> message",
    "< message",
    "> close",
    "< close",
  ]);
});
*/
