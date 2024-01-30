import {PIECE_DEFINITIONS} from './piece-definitions';
import type {PieceModel} from './piece-model';

export type Position = {
  x: number;
  y: number;
};

export type Size = {
  width: number;
  height: number;
};

export type BoundingBox = Position & Size;

export const now = () => new Date().getTime();

export const rotatePosition = (
  position: Position,
  around: Position,
  radians: number,
) => {
  const {x, y} = position;
  const {x: cx, y: cy} = around;
  const cos = Math.cos(radians);
  const sin = Math.sin(radians);
  const nx = cos * (x - cx) + sin * (y - cy) + cx;
  const ny = cos * (y - cy) - sin * (x - cx) + cy;
  return {x: nx, y: ny};
};

export const center = (box: BoundingBox) => ({
  x: box.x + box.width / 2,
  y: box.y + box.height / 2,
});

export const addRadians = (value: number, add: number) => {
  const c = Math.PI * 2;
  return (c + value + add) % c;
};

export const getAngle = (center: Position, to: Position) =>
  Math.atan2(to.y - center.y, to.x - center.x);

export function must<T>(val: T | undefined | null, name?: string): T {
  if (val === undefined || val === null) {
    throw new Error(`assertion error: ${name ? name : 'val must be defined'}`);
  }
  return val;
}

export const randFloat = (min: number, max: number) =>
  Math.random() * (max - min) + min;

export const randInt = (min: number, max: number) => {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.round(Math.random() * (max - min) + min);
};

export const randElm = <T>(arr: Readonly<T[]>) => arr[randInt(0, arr.length)];

export const randIndices = (count: number, max: number) => {
  const res: number[] = [];
  while (res.length < count) {
    for (;;) {
      const cand = randInt(0, max);
      if (!res.includes(cand)) {
        res.push(cand);
        break;
      }
    }
  }
  return res;
};

export const distance = (a: Position, b: Position): number => {
  const x = b.x - a.x;
  const y = b.y - a.y;
  return Math.sqrt(x * x + y * y);
};

export function getStage(home: Rect | null) {
  if (!home) {
    return null;
  }
  const gutterBase = 32;
  const gutterX = gutterBase * 3;
  const gutterTop = gutterBase * 2.25;
  const gutterBottom = gutterBase * 2.25;
  return new Rect(
    home.x - gutterX,
    home.y - gutterTop,
    home.width + gutterX * 2,
    home.height + gutterTop + gutterBottom,
  );
}

export function generateRandomPieces(home: Rect, stage: Rect) {
  const approxPieceSize = 50;
  const res: PieceModel[] = [];

  // This uses Mitchell's best candidate algorithm to generate the initial
  // positions for the puzzle: https://gist.github.com/mbostock/1893974.
  // The idea, roughly, is to loop through each piece and choose a random
  // position that's farthese from other pieces.

  const getCandidates = () => {
    const cands = new Array<{pos: Position; minDist: number}>();
    // Most of the time put the pieces outside the letters, so you can still
    // read "alive", but it looks nicer if a few are allowed inside.
    const allowCandidateOverLetters = Math.random() < 0;
    while (cands.length < 20) {
      const pos = {
        x: randFloat(stage.left(), stage.right() - approxPieceSize),
        y: randFloat(stage.top(), stage.bottom() - approxPieceSize),
      };
      if (!allowCandidateOverLetters) {
        if (
          pos.x >= home.left() &&
          pos.x <= home.right() &&
          pos.y >= home.top() &&
          pos.y <= home.bottom()
        ) {
          continue;
        }
      }
      let minDist = Infinity;
      for (const selectedPos of res) {
        const d = distance(selectedPos, pos);
        if (d < minDist) {
          minDist = d;
        }
      }
      cands.push({pos, minDist});
    }
    return cands;
  };

  const initiallyPlacedPieces = new Array<number>();
  for (const letter of ['A', 'L', 'I', 'V', 'E']) {
    const first = PIECE_DEFINITIONS.findIndex(def => def.letter === letter);
    const pieces = PIECE_DEFINITIONS.filter(def => def.letter === letter);
    const numPlacedPieces = randInt(1, 2);
    const placedIndicies = randIndices(numPlacedPieces, pieces.length - 1);
    for (const idx of placedIndicies) {
      initiallyPlacedPieces.push(first + idx);
    }
  }

  for (let i = 0; i < PIECE_DEFINITIONS.length; i++) {
    const def = PIECE_DEFINITIONS[i];
    if (initiallyPlacedPieces.includes(i)) {
      res.push({
        id: i.toString(),
        ...coordinateToPosition(def, home, stage),
        rotation: 0,
        handleRotation: 0,
        placed: true,
      });
      continue;
    }

    const candidates = getCandidates();
    const farthest = candidates.reduce((best, cand) => {
      if (cand.minDist > best.minDist) {
        return cand;
      }
      return best;
    }, candidates[0]);
    const {pos} = farthest;
    const newPiece: PieceModel = {
      id: i.toString(),
      ...pos,
      rotation: randFloat(0, Math.PI * 2),
      handleRotation: -Math.PI / 2, // north
      placed: false,
    };
    res.push(newPiece);
  }

  return res.map(p => ({
    ...p,
    ...positionToCoordinate(p, home, stage),
  }));
}

// Our coordinate system has 2 requirements:
// 1. We want pieces to be able to use the full horizontal width of the screen
// on wider screens, even when that means they are far outside the home location
// of letters.
// 2. No piece should be positioned off screen on any user's device.
// 3. When a piece is over a letter, it should be over the same part of the
// letter on every other user's screen. Otherwise when users drop pieces, other
// users with different sized screens will see the letter jump.
// Requirement 3 contradicts 1 and 2, so we use 2 overlaying systems.
// - Coordinates are represented as numbers from -1 to 2
// - Coordinates between 0 and 1 are divided by the size of the "precise area",
// and offset by the top/left of it - so cursors inside this area will
// represent the same scaled position on all screens.
// - Coordinates outside of that area (-1-0 and 1-2) are used as a percentage of
// the remaining area, so they will never go outside the screen area. So on
// larger screens, cursors will move slower than on bigger ones, and pieces will
// get closer or further away from the demo when you resize your screen.
export const positionToCoordinate = (
  position: Position,
  home: Rect,
  stage: Rect,
) => {
  let x: number;
  if (position.x < home.x) {
    const gutterWidth = home.x - stage.x;
    const posWithinGutter = position.x - stage.x;
    x = posWithinGutter / gutterWidth - 1;
  } else if (position.x > home.right()) {
    const gutterWidth = stage.right() - home.right();
    const posWithinGutter = position.x - home.right();
    x = posWithinGutter / gutterWidth + 1;
  } else {
    x = (position.x - home.x) / home.width;
  }
  let y: number;
  if (position.y < home.y) {
    const gutterHeight = home.y - stage.y;
    const posWithinGutter = position.y - stage.y;
    y = posWithinGutter / gutterHeight - 1;
  } else if (position.y > home.bottom()) {
    const gutterHeight = stage.bottom() - home.bottom();
    const posWithinGutter = position.y - home.bottom();
    y = posWithinGutter / gutterHeight + 1;
  } else {
    y = (position.y - home.y) / home.height;
  }
  return {x, y};
};

export const coordinateToPosition = (
  coord: Position,
  home: Rect,
  stage: Rect,
) => {
  let x = -1;
  if (coord.x < 0) {
    // translate coord back into domain [0..1] then multiply by left margin.
    const gutterWidth = home.x - stage.x;
    const posWithinGutter = (coord.x + 1) * gutterWidth;
    x = stage.x + posWithinGutter;
  } else if (coord.x > 1) {
    // same for right margin.
    const gutterWidth = stage.right() - home.right();
    const posWithinGutter = (coord.x - 1) * gutterWidth;
    x = home.right() + posWithinGutter;
  } else {
    x = home.x + coord.x * home.width;
  }

  // same for bottom
  let y = -1;
  if (coord.y < 0) {
    const gutterHeight = home.y - stage.y;
    const posWithinGutter = (coord.y + 1) * gutterHeight;
    y = stage.y + posWithinGutter;
  } else if (coord.y > 1) {
    const gutterHeight = stage.bottom() - home.bottom();
    const posWithinGutter = (coord.y - 1) * gutterHeight;
    y = home.bottom() + posWithinGutter;
  } else {
    y = home.y + coord.y * home.height;
  }
  return {x, y};
};

export const getAbsoluteRect = (element: Element) => {
  const cr = element.getBoundingClientRect();
  return new Rect(
    cr.left + element.ownerDocument.documentElement.scrollLeft,
    cr.top + element.ownerDocument.documentElement.scrollTop,
    cr.width,
    cr.height,
  );
};

export class Rect {
  constructor(
    readonly x: number,
    readonly y: number,
    readonly width: number,
    readonly height: number,
  ) {}
  left() {
    return this.x;
  }
  top() {
    return this.y;
  }
  right() {
    return this.x + this.width;
  }
  bottom() {
    return this.y + this.height;
  }
}

export const simpleHash = (s: string) => {
  let hash = 0,
    i,
    chr;
  if (s.length === 0) return hash;
  for (i = 0; i < s.length; i++) {
    chr = s.charCodeAt(i);
    hash = (hash << 5) - hash + chr;
    hash |= 0; // Convert to 32bit integer
  }
  return hash;
};
