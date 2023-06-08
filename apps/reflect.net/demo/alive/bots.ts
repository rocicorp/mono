import type {ClientID, Reflect} from '@rocicorp/reflect/client';
import {nanoid} from 'nanoid';
import type {M} from '../shared/mutators';
import {BotMove, BotRecording, BotType, botRecordings} from './bot-recordings';
import {getBotController} from './client-model';
import {colorToString, idToColor} from './colors';
import type {PieceInfo} from './piece-info';
import {handleDrag, selectIfAvailable} from './puzzle-biz';
import {
  Position,
  Rect,
  coordinateToPosition,
  positionToCoordinate,
} from './util';
type BotPlayback = {
  startTime: number;
  moveIndex: number;
  timeShift: number;
  type: BotType;
  dragInfo?: {pieceID: string; offset: Position; start: number} | undefined;
  moves: BotMove[];
  manuallyTriggeredBot: boolean;
};

import {Location, getLocationString} from '@/util/get-location-string';

const botLocations: Location[] = [
  {
    country: 'US',
    city: 'San Francisco',
    region: 'CA',
  },
  {
    country: 'US',
    city: 'Atlanta',
    region: 'GA',
  },
  {
    country: 'US',
    city: 'Los Angeles ',
    region: 'CA',
  },
  {
    country: 'US',
    city: 'New York ',
    region: 'NY',
  },
  {
    country: 'US',
    city: 'Seattle',
    region: 'WA',
  },
  {
    country: 'GB',
    city: 'London',
    region: '',
  },
  {
    country: 'FR',
    city: 'Paris',
    region: '',
  },
];

export class Bots {
  private _home: Rect;
  private _stage: Rect;
  private _currentRecording:
    | {
        start: number;
        moves: BotMove[];
      }
    | undefined = undefined;
  private _lastRecording: BotMove[] | undefined = undefined;
  private _botPlaybackByClientID: Map<string, BotPlayback> = new Map();
  private readonly _cleanup: () => void;
  private _raf: number = 0;

  private _r: Reflect<M>;
  private _clientID: ClientID | undefined = undefined;
  private _isBotController = false;
  private _pieces: Record<string, PieceInfo> | undefined = undefined;

  constructor(r: Reflect<M>, home: Rect, stage: Rect) {
    this._r = r;
    this._home = home;
    this._stage = stage;

    const handlePointerMove = (e: PointerEvent) => {
      if (this._currentRecording) {
        const coord = positionToCoordinate(
          {x: e.pageX, y: e.pageY},
          this._home,
          this._stage,
        );
        this._currentRecording.moves.push({
          time: performance.now() - this._currentRecording.start,
          coordX: coord.x,
          coordY: coord.y,
        });
      }
    };

    window.addEventListener('pointermove', handlePointerMove);
    const handleKeyPress = (e: KeyboardEvent) => {
      if (!this._clientID) {
        return;
      }
      if (e.key === '`') {
        if (this._currentRecording) {
          console.log(
            JSON.stringify(
              {
                clientID: 'bot-' + nanoid(),
                moves: this._currentRecording.moves,
              },
              undefined,
              2,
            ),
          );
          this._lastRecording = this._currentRecording.moves;
          this._currentRecording = undefined;
        } else {
          console.log('starting recording');
          this._currentRecording = {
            start: performance.now(),
            moves: [],
          };
        }
      }
      if (e.key === '!' || (e.key === '@' && this._lastRecording)) {
        const multiply =
          new URL(window.location.href).searchParams.get('mult') ?? '1';
        for (let i = 0; i < parseInt(multiply); i++) {
          for (const bot of e.key === '@' && this._lastRecording
            ? [{clientID: 'latest-bot', moves: this._lastRecording}]
            : botRecordings) {
            const transformedMoves = bot.moves.map(move => ({
              time: move.time,
              coordX: move.coordX - i * 0.1,
              coordY: move.coordY - i * 0.2,
            }));
            const botClientID = bot.clientID + i;
            this._startPlayback(
              this._clientID,
              botClientID,
              transformedMoves,
              'puzzle',
              true,
              i * 500,
            );
          }
        }
        this._maybeRaf();
      }
    };
    window.addEventListener('keypress', handleKeyPress);

    const maybeLaunchBots = () => {
      if (this._clientID && this._isBotController) {
        const currentNumPuzzleBots = [
          ...this._botPlaybackByClientID.values(),
        ].filter(p => p.type === 'puzzle').length;
        const currentNumWanderBots = [
          ...this._botPlaybackByClientID.values(),
        ].filter(p => p.type === 'wander').length;

        let toPlayback: BotRecording[] = [];
        const shuffled = shuffle(botRecordings);
        if (
          currentNumPuzzleBots === 0 ||
          (currentNumPuzzleBots === 1 && Math.random() > 0.9)
        ) {
          toPlayback.push(shuffled.find(p => p.type === 'puzzle')!);
        }

        if (
          currentNumWanderBots === 0 ||
          (currentNumPuzzleBots === 1 && Math.random() > 0.9)
        ) {
          toPlayback.push(shuffled.find(p => p.type === 'wander')!);
        }

        for (const bot of toPlayback) {
          this._startPlayback(
            this._clientID,
            bot.clientID,
            bot.moves,
            bot.type,
            false,
          );
        }
        this._maybeRaf();
      }
    };

    // Launch them once at startup, then every 3 seconds.
    maybeLaunchBots();
    const interval = setInterval(maybeLaunchBots, 3_000);

    r.clientID.then(clientID => (this._clientID = clientID));
    const cleanupSubscribe = r.subscribe(
      async tx => {
        const botController = (await getBotController(tx)) ?? null;
        const isBotController = botController?.clientID === tx.clientID;
        return {
          isBotController,
        };
      },
      {
        onData: result => {
          console.log('botController change', result);
          this._isBotController = result.isBotController;
          if (this._isBotController) {
            // Don't wait for 3s to expire to launch bots.
            maybeLaunchBots();
          } else {
            // No longer bot controller, stop playing bots
            for (const [botClientID, {manuallyTriggeredBot}] of this
              ._botPlaybackByClientID) {
              if (!manuallyTriggeredBot) {
                this._botPlaybackByClientID.delete(botClientID);
              }
            }
          }
        },
      },
    );

    this._cleanup = () => {
      window.removeEventListener('pointermove', handlePointerMove);
      window.removeEventListener('keypress', handleKeyPress);
      clearInterval(interval);
      cleanupSubscribe();
    };
  }

  private _startPlayback(
    clientID: string,
    botClientID: string,
    moves: BotMove[],
    type: BotType,
    manuallyTriggeredBot: boolean,
    timeShift = 0,
  ) {
    this._r.mutate.putClient({
      x: moves[0]?.coordX ?? 0,
      y: moves[0]?.coordY ?? 0,
      id: botClientID,
      selectedPieceID: '',
      color: colorToString(idToColor(botClientID)),
      location: getLocationString(
        botLocations[Math.floor(Math.random() * botLocations.length)],
      ),
      focused: true,
      botControllerID: clientID,
      manuallyTriggeredBot,
    });
    this._botPlaybackByClientID.set(botClientID, {
      startTime: performance.now(),
      moveIndex: 0,
      timeShift: timeShift,
      moves: moves,
      manuallyTriggeredBot,
      type,
    });
  }

  private _maybeRaf() {
    for (const [botClientID, {moveIndex, moves}] of this
      ._botPlaybackByClientID) {
      if (moveIndex >= moves.length) {
        this._botPlaybackByClientID.delete(botClientID);
        this._r.mutate.deleteClient(botClientID);
      }
    }
    if (this._botPlaybackByClientID.size > 0) {
      this._raf = requestAnimationFrame(this._onRaf);
    }
  }

  private _onRaf = async () => {
    for (const [botClientID, playback] of this._botPlaybackByClientID) {
      const now = performance.now() - playback.startTime;
      let move = undefined;
      for (
        ;
        playback.moveIndex < playback.moves.length &&
        playback.moves[playback.moveIndex].time + playback.timeShift <= now;
        playback.moveIndex++
      ) {
        move = playback.moves[playback.moveIndex];
      }
      if (move) {
        const updated = await this._r.mutate.updateClient({
          x: move.coordX,
          y: move.coordY,
          id: botClientID,
        });
        if (!updated) {
          console.log(
            'failed to update bot client',
            botClientID,
            'stopping playback',
          );
          this._botPlaybackByClientID.delete(botClientID);
          continue;
        }

        if (playback.dragInfo) {
          // If bot has been dragging for at least 5 seconds,
          // 90% chance it drops the piece
          if (now - playback.dragInfo.start > 5_000 && Math.random() > 0.9) {
            const updated = this._r.mutate.updateClient({
              id: botClientID,
              selectedPieceID: '',
            });
            if (!updated) {
              console.log(
                'failed to update bot client',
                botClientID,
                'stopping playback',
              );
              this._botPlaybackByClientID.delete(botClientID);
              continue;
            }
            playback.dragInfo = undefined;
          } else {
            const position = coordinateToPosition(
              {
                x: move.coordX,
                y: move.coordY,
              },
              this._home,
              this._stage,
            );
            if (
              this._pieces &&
              handleDrag(
                botClientID,
                {pageX: position.x, pageY: position.y},
                this._pieces[playback.dragInfo.pieceID],
                playback.dragInfo.offset,
                this._r,
                this._home,
                this._stage,
              )
            ) {
              // piece snapped and selection was cleared
              playback.dragInfo = undefined;
            }
          }
          // If bot is not currently dragging a piece, 10% of the
          // time hit test to see if bot cursor is over a piece, if
          // it is the bot begins dragging it.
        } else if (Math.random() > 0.9) {
          const position = coordinateToPosition(
            {
              x: move.coordX,
              y: move.coordY,
            },
            this._home,
            this._stage,
          );
          const el = document.elementFromPoint(position.x, position.y);
          if (el) {
            const pieceID = el.getAttribute('data-pieceid');
            if (pieceID && this._pieces && !playback.dragInfo) {
              const piece = this._pieces[pieceID];
              if (piece && selectIfAvailable(botClientID, piece, this._r)) {
                // Pause bot for 200 - 350 ms to simulate the small
                // pause humans make when selecting a piece (pause is done
                // by time shifting remaining moves).  Otherwise
                // the bot can pick up a piece while its cursor
                // is quickly moving over it, which looks unnatural.
                playback.timeShift += Math.random() * 150 + 350;
                const piecePos = coordinateToPosition(
                  {x: piece.x, y: piece.y},
                  this._home,
                  this._stage,
                );
                playback.dragInfo = {
                  pieceID,
                  offset: {
                    x: position.x - piecePos.x,
                    y: position.y - piecePos.y,
                  },
                  start: now,
                };
              }
            }
          }
        }
      }
    }
    this._maybeRaf();
  };

  async setPieces(pieces: Record<string, PieceInfo>) {
    this._pieces = pieces;
  }

  async handleResize(home: Rect, stage: Rect) {
    this._home = home;
    this._stage = stage;
  }

  cleanup() {
    this._cleanup();
    cancelAnimationFrame(this._raf);
  }
}

function shuffle<T>(arr: Iterable<T>): T[] {
  const shuffled = [...arr];
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
  }
  return shuffled;
}
