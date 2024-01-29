import type {Reflect} from '@rocicorp/reflect/client';
import {event} from 'nextjs-google-analytics';
import type {M} from '../shared/mutators';
import {PIECE_DEFINITIONS, PieceDefinition} from './piece-definitions';
import type {PieceInfo} from './piece-info';
import type {PieceModel} from './piece-model';
import {
  Position,
  Rect,
  coordinateToPosition,
  distance,
  positionToCoordinate,
} from './util';

export const handleDrag = (
  e: {pageX: number; pageY: number},
  piece: PieceModel,
  offset: Position,
  r: Reflect<M>,
  home: Rect,
  stage: Rect,
  botID?: string | undefined,
) => {
  const def = PIECE_DEFINITIONS[parseInt(piece.id)];

  const pos = {
    x: e.pageX - offset.x,
    y: e.pageY - offset.y,
  };

  if (pos.x < stage.x) {
    pos.x = stage.x;
  }
  if (pos.y < stage.y) {
    pos.y = stage.y;
  }
  if (pos.x + def.width > stage.right()) {
    pos.x = stage.right() - def.width;
  }
  if (pos.y + def.height > stage.bottom()) {
    pos.y = stage.bottom() - def.height;
  }

  const coordinate = positionToCoordinate(pos, home, stage);
  r.mutate.updatePiece({id: piece.id, ...coordinate});

  if (checkSnap(piece, def, pos, r, home, stage)) {
    if (botID) {
      r.mutate.updateBot({id: botID, selectedPieceID: ''});
    } else {
      r.mutate.updateClient({selectedPieceID: ''});
    }
    event('alive_snap_piece', {
      category: 'Alive Demo',
      action: 'Snap puzzle piece',
      label: 'Demo',
    });
    return true;
  }
  return false;
};

export function checkSnap(
  piece: PieceModel,
  def: PieceDefinition,
  currPos: Position,
  r: Reflect<M>,
  home: Rect,
  stage: Rect,
) {
  const homePos = coordinateToPosition(def, home, stage);
  const dist = distance(currPos, homePos);
  const distThresh = 10;
  const rotThresh = Math.PI / 6;
  if (
    dist <= distThresh &&
    (piece.rotation <= rotThresh || Math.PI * 2 - piece.rotation <= rotThresh)
  ) {
    r.mutate.updatePiece({
      id: piece.id,
      x: def.x,
      y: def.y,
      rotation: 0,
      placed: true,
    });
    return true;
  }
  return false;
}

export function selectIfAvailable(
  clientID: string,
  type: 'client' | 'bot',
  piece: PieceInfo,
  r: Reflect<M>,
) {
  if (piece.placed) {
    console.debug('cannot select already placed pieces');
    return false;
  }

  if (piece.selector === clientID) {
    // already selected by this client, nothing to do.
    return true;
  }

  if (piece.selector !== null) {
    console.debug(
      `${type} ${clientID} cannot select piece ${piece.id}, already selected by ${piece.selector}}`,
    );
    return false;
  }

  if (type === 'client') {
    r.mutate.updateClient({selectedPieceID: piece.id});
  } else {
    r.mutate.updateBot({id: clientID, selectedPieceID: piece.id});
  }
  r.mutate.updatePiece({id: piece.id, handleRotation: -Math.PI / 2});
  return true;
}
