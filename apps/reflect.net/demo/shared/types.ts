// Per-frame application state. We query reflect directly for this data every
// time we draw.
export type State = {
  actorID: ActorID;
  cursors: Record<ActorID, Cursor>;
  pieces: ActivePuzzlePiece[];
};

export type Debug = {
  fps: number;
};

export type ActorID = string;

export type PieceNumber = number;
// Note that this will be a huge number
export type PieceOrder = number;

export type Color = [number, number, number]; // rgb

export type Actor = {
  id: ActorID;
  colorIndex: number;
  room: string;
  isBot: boolean;
  botController: string | null;
  location: string | null;
};

export enum Letter {
  A = 'A',
  L = 'L',
  I = 'I',
  V = 'V',
  E = 'E',
}

// Each actor has a cursor. They are positioned in global space, so we also need
// to send the space around so we can draw them relatively.
export enum TouchState {
  Unknown = 0,
  Touching = 1,
  Clicking = 2,
}
export type Cursor = Position & {
  actorID: ActorID;
  onPage: boolean;
  ts: number;
  isDown: boolean;
  touchState: TouchState;
  activePiece: PieceNumber;
};

export type ActivePuzzlePiece = PuzzlePiece &
  Position & {
    number: PieceNumber;
    rotation: number;
    placed: boolean;
    handlePosition: Position;
    moverID: string;
    rotatorID: string;
  };

export type PuzzlePiece = Size & {
  letter: Letter;
  paths: string[];
  dx: number;
  dy: number;
};

export type BoundingBox = Position & Size;

export type Size = {
  width: number;
  height: number;
};

// In this app, all position values are between 0 and 1, and expected to be
// multiplied by window.innerWidth/window.innerHeight when used in rendering
// code.
export type Position = {
  x: number;
  y: number;
};

export enum Env {
  CLIENT,
  SERVER,
}

// Bot Stuff
export type RecordingID = string;
export type BroadcastID = string;
export enum RecordingType {
  BROWSE,
  FIND,
  PLACE,
  ROTATE,
}
export type RecordingInfo = {
  type: RecordingType;
  startCoord?: Position;
  endCoord?: Position;
};
export type BroadcastQueue = BroadcastInfo & {
  recordings: RecordingBroadcast[];
};
export type Broadcast = BroadcastInfo &
  RecordingBroadcast & {broadcastId: BroadcastID};
export type RecordingBroadcast = {
  recordingId: RecordingID;
  type: RecordingType;
  pieceNum?: PieceNumber;
  targetCoord?: Position;
  angle?: number;
  scale?: number;
};
export type BroadcastInfo = {
  roomId: string;
  broadcasterId: ActorID;
  botId: ActorID;
  colorIdx: number;
};
export type RecordingCursor = {
  x: number;
  y: number;
  t: number; // timestamp
  o: boolean; // on page
  d: boolean; // is down
};

export type Latency = 0 | 1 | 2;
