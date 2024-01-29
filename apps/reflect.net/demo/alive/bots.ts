import type {ClientID} from '@rocicorp/reflect';
import type {Reflect} from '@rocicorp/reflect/client';
import {nanoid} from 'nanoid';
import type {M} from '../shared/mutators';
import type {BotMove, BotRecording, BotType} from './bot-recordings';
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

async function getBotRecordings(): Promise<BotRecording[]> {
  return (await import('./bot-recordings')).botRecordings;
}

export class Bots {
  #home: Rect;
  #stage: Rect;
  #currentRecording:
    | {
        start: number;
        moves: BotMove[];
      }
    | undefined = undefined;
  #lastRecording: BotMove[] | undefined = undefined;
  #botPlaybackByID: Map<string, BotPlayback> = new Map();
  readonly #cleanup: () => void;
  #raf = 0;

  readonly #r: Reflect<M>;
  readonly #clientID: ClientID;
  #isBotController = false;
  #pieces: Record<string, PieceInfo> | undefined = undefined;

  constructor(r: Reflect<M>, home: Rect, stage: Rect) {
    this.#r = r;
    this.#clientID = r.clientID;
    this.#home = home;
    this.#stage = stage;

    const handlePointerMove = (e: PointerEvent) => {
      if (this.#currentRecording) {
        const coord = positionToCoordinate(
          {x: e.pageX, y: e.pageY},
          this.#home,
          this.#stage,
        );
        this.#currentRecording.moves.push({
          time: performance.now() - this.#currentRecording.start,
          coordX: coord.x,
          coordY: coord.y,
        });
      }
    };

    window.addEventListener('pointermove', handlePointerMove);
    const handleKeyPress = async (e: KeyboardEvent) => {
      if (e.key === '`') {
        if (this.#currentRecording) {
          console.log(
            JSON.stringify(
              {
                clientID: 'bot-' + nanoid(),
                moves: this.#currentRecording.moves,
              },
              undefined,
              2,
            ),
          );
          this.#lastRecording = this.#currentRecording.moves;
          this.#currentRecording = undefined;
        } else {
          console.log('starting recording');
          this.#currentRecording = {
            start: performance.now(),
            moves: [],
          };
        }
      }
      if (e.key === '!' || (e.key === '@' && this.#lastRecording)) {
        const multiply =
          new URL(window.location.href).searchParams.get('mult') ?? '1';
        for (let i = 0; i < parseInt(multiply); i++) {
          for (const bot of e.key === '@' && this.#lastRecording
            ? [{clientID: 'latest-bot', moves: this.#lastRecording}]
            : await getBotRecordings()) {
            const transformedMoves = bot.moves.map(move => ({
              time: move.time,
              coordX: move.coordX - i * 0.1,
              coordY: move.coordY - i * 0.2,
            }));
            const botID = bot.clientID + i;
            this.#startPlayback(
              botID,
              transformedMoves,
              'puzzle',
              true,
              i * 500,
            );
          }
        }
        this.#maybeRaf();
      }
    };
    window.addEventListener('keypress', handleKeyPress);

    const maybeLaunchBots = async () => {
      if (this.#isBotController) {
        const currentNumPuzzleBots = [...this.#botPlaybackByID.values()].filter(
          p => p.type === 'puzzle',
        ).length;
        const currentNumWanderBots = [...this.#botPlaybackByID.values()].filter(
          p => p.type === 'wander',
        ).length;

        const toPlayback: BotRecording[] = [];
        const shuffled = shuffle(await getBotRecordings());
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
          this.#startPlayback(bot.clientID, bot.moves, bot.type, false);
        }
        this.#maybeRaf();
      }
    };

    // Launch them once at startup, then every 3 seconds.
    void maybeLaunchBots();
    const interval = setInterval(maybeLaunchBots, 3_000);

    const cleanupSubscribe = r.subscribe(
      async tx => {
        const botController = (await getBotController(tx)) ?? null;
        const isBotController = botController?.clientID === tx.clientID;
        return {
          isBotController,
        };
      },
      {
        onData: async result => {
          console.log('botController change', result);
          this.#isBotController = result.isBotController;
          if (this.#isBotController) {
            // Don't wait for 3s to expire to launch bots.
            await maybeLaunchBots();
          } else {
            // No longer bot controller, stop playing bots
            for (const [botID, {manuallyTriggeredBot}] of this
              .#botPlaybackByID) {
              if (!manuallyTriggeredBot) {
                this.#botPlaybackByID.delete(botID);
              }
            }
          }
        },
      },
    );

    this.#cleanup = () => {
      window.removeEventListener('pointermove', handlePointerMove);
      window.removeEventListener('keypress', handleKeyPress);
      clearInterval(interval);
      cleanupSubscribe();
    };
  }

  #startPlayback(
    botID: string,
    moves: BotMove[],
    type: BotType,
    manuallyTriggeredBot: boolean,
    timeShift = 0,
  ) {
    this.#r.mutate.setBot({
      x: moves[0]?.coordX ?? 0,
      y: moves[0]?.coordY ?? 0,
      id: botID,
      selectedPieceID: '',
      color: colorToString(idToColor(botID)),
      location: getLocationString(
        botLocations[Math.floor(Math.random() * botLocations.length)],
      ),
      focused: true,
      botControllerID: this.#clientID,
      manuallyTriggeredBot,
    });
    this.#botPlaybackByID.set(botID, {
      startTime: performance.now(),
      moveIndex: 0,
      timeShift,
      moves,
      manuallyTriggeredBot,
      type,
    });
  }

  #maybeRaf() {
    for (const [botID, {moveIndex, moves}] of this.#botPlaybackByID) {
      if (moveIndex >= moves.length) {
        this.#botPlaybackByID.delete(botID);
        this.#r.mutate.deleteBot(botID);
      }
    }
    if (this.#botPlaybackByID.size > 0) {
      this.#raf = requestAnimationFrame(this.#onRaf);
    }
  }

  #onRaf = async () => {
    for (const [botID, playback] of this.#botPlaybackByID) {
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
        const updated = await this.#r.mutate.updateBot({
          x: move.coordX,
          y: move.coordY,
          id: botID,
        });
        if (!updated) {
          console.log(
            'failed to update bot client',
            botID,
            'stopping playback',
          );
          this.#botPlaybackByID.delete(botID);
          continue;
        }

        if (playback.dragInfo) {
          // If bot has been dragging for at least 5 seconds,
          // 90% chance it drops the piece
          if (now - playback.dragInfo.start > 5_000 && Math.random() > 0.9) {
            const updated = this.#r.mutate.updateBot({
              id: botID,
              selectedPieceID: '',
            });
            if (!updated) {
              console.log(
                'failed to update bot client',
                botID,
                'stopping playback',
              );
              this.#botPlaybackByID.delete(botID);
              continue;
            }
            playback.dragInfo = undefined;
          } else {
            const position = coordinateToPosition(
              {
                x: move.coordX,
                y: move.coordY,
              },
              this.#home,
              this.#stage,
            );
            if (
              this.#pieces &&
              handleDrag(
                {pageX: position.x, pageY: position.y},
                this.#pieces[playback.dragInfo.pieceID],
                playback.dragInfo.offset,
                this.#r,
                this.#home,
                this.#stage,
                botID,
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
            this.#home,
            this.#stage,
          );
          const el = document.elementFromPoint(position.x, position.y);
          if (el) {
            const pieceID = el.getAttribute('data-pieceid');
            if (pieceID && this.#pieces && !playback.dragInfo) {
              const piece = this.#pieces[pieceID];
              if (piece && selectIfAvailable(botID, 'bot', piece, this.#r)) {
                // Pause bot for 200 - 350 ms to simulate the small
                // pause humans make when selecting a piece (pause is done
                // by time shifting remaining moves).  Otherwise
                // the bot can pick up a piece while its cursor
                // is quickly moving over it, which looks unnatural.
                playback.timeShift += Math.random() * 150 + 350;
                const piecePos = coordinateToPosition(
                  {x: piece.x, y: piece.y},
                  this.#home,
                  this.#stage,
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
    this.#maybeRaf();
  };

  setPieces(pieces: Record<string, PieceInfo>) {
    this.#pieces = pieces;
  }

  handleResize(home: Rect, stage: Rect) {
    this.#home = home;
    this.#stage = stage;
  }

  cleanup() {
    this.#cleanup();
    cancelAnimationFrame(this.#raf);
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
