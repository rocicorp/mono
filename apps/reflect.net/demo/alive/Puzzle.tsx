import {useIsomorphicLayoutEffect} from '@/hooks/use-isomorphic-layout-effect';
import {useEventTimeout} from '@/hooks/use-timeout';
import type {Reflect} from '@rocicorp/reflect/client';
import {useSubscribe} from '@rocicorp/reflect/react';
import {useRef} from 'react';
import type {M} from '../shared/mutators';
import {Piece} from './Piece';
import {Bots} from './bots';
import {getClient} from './client-model';
import {
  PIECE_DEFINITIONS,
  PieceDefinition,
  SVG_ORIGINAL_SIZE,
} from './piece-definitions';
import {PieceInfo, getPieceInfos} from './piece-info';
import type {PieceModel} from './piece-model';
import {
  checkSnap as sharedCheckSnap,
  handleDrag as sharedHandleDrag,
  selectIfAvailable as sharedSelectIfAvailable,
} from './puzzle-biz';
import {
  Position,
  Rect,
  addRadians,
  center,
  coordinateToPosition,
  getAngle,
} from './util';

export function Puzzle({
  r,
  presentClientIDs,
  home,
  stage,
  setBodyClass,
}: {
  r: Reflect<M>;
  presentClientIDs: ReadonlySet<string>;
  home: Rect;
  stage: Rect;
  setBodyClass: (cls: string, enabled: boolean) => void;
}) {
  const {pieces, myClient} = useSubscribe(
    r,
    async tx => ({
      pieces: await getPieceInfos(tx, presentClientIDs),
      myClient: (await getClient(tx, tx.clientID)) ?? null,
    }),
    {pieces: {}, myClient: null},
    [presentClientIDs],
  );

  const ref = useRef<HTMLDivElement>(null);
  const dragging = useRef(
    new Map<number, {pieceID: string; offset: Position}>(),
  );
  const rotating = useRef(
    new Map<number, {pieceID: string; radOffset: number}>(),
  );

  const isMouseDown = useRef(false);

  const handlePieceHover = (model: PieceInfo, event: React.PointerEvent) => {
    // only select if topmost piece at this position
    if (
      document
        .elementFromPoint(event.clientX, event.clientY)
        ?.getAttribute('data-pieceid') !== model.id
    ) {
      return;
    }
    if (selectIfAvailable(model)) {
      setBodyClass('grab', true);
      scheduleBlur();
    }
  };

  const [setBlurTimeout, clearBlurTimeout] = useEventTimeout();

  const scheduleBlur = () => {
    setBlurTimeout(() => {
      if (!isMouseDown.current) {
        setBodyClass('grab', false);
        r.mutate.updateClient({selectedPieceID: ''});
      }
    }, 1000);
  };

  const cancelBlur = () => {
    setBodyClass('grab', false);
    clearBlurTimeout();
  };

  const selectIfAvailable = (model: PieceInfo) => {
    if (!myClient) {
      return;
    }
    return sharedSelectIfAvailable(myClient.id, 'client', model, r);
  };

  const handlePiecePointerDown = (
    model: PieceInfo,
    event: React.PointerEvent,
    piecePos: Position,
  ) => {
    if (selectIfAvailable(model)) {
      isMouseDown.current = true;
      setBodyClass('grab', true);
      scheduleBlur();
    } else {
      return;
    }
    ref.current!.setPointerCapture(event.pointerId);
    dragging.current.set(event.pointerId, {
      pieceID: model.id,
      offset: {
        x: event.pageX - piecePos.x,
        y: event.pageY - piecePos.y,
      },
    });
    setBodyClass('grabbing', true);
  };
  useIsomorphicLayoutEffect(() => {
    const handlePointerDown = () => {
      // clear selection when clicking outside of a piece
      // the pointerdown handler inside piece cancels bubbling
      cancelBlur();
      r.mutate.updateClient({selectedPieceID: ''});
    };
    window.addEventListener('pointerdown', handlePointerDown);
    return () => {
      window.removeEventListener('pointerdown', handlePointerDown);
    };
  }, [r, myClient]);

  const botsRef = useRef<Bots>();
  useIsomorphicLayoutEffect(() => {
    const bots = new Bots(r, home, stage);
    bots.setPieces(pieces);
    botsRef.current = bots;
    return () => bots.cleanup();
    // home, screenSize and stage changing our handled below
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [r]);
  useIsomorphicLayoutEffect(() => {
    botsRef.current?.handleResize(home, stage);
  }, [home, stage]);
  useIsomorphicLayoutEffect(() => {
    botsRef.current?.setPieces(pieces);
  }, [pieces]);

  const handlePointerMove = (e: React.PointerEvent) => {
    const dragInfo = dragging.current.get(e.pointerId);
    if (dragInfo) {
      handleDrag(e, dragInfo);
      scheduleBlur();
    }

    const rotateInfo = rotating.current.get(e.pointerId);
    if (rotateInfo) {
      handleRotate(e, rotateInfo);
      scheduleBlur();
    }
  };

  const handleDrag = (
    e: React.PointerEvent,
    dragInfo: {pieceID: string; offset: Position},
  ) => {
    /*
    const elm = document.createElement('div');
    elm.style.position = 'absolute';
    elm.style.left = e.pageX - 1 + 'px';
    elm.style.top = e.pageY - 1 + 'px';
    elm.style.width = '2px';
    elm.style.height = '2px';
    elm.style.backgroundColor = 'green';
    elm.style.zIndex = '2';
    elm.style.opacity = '0.2';
    elm.style.pointerEvents = 'none';
    document.body.appendChild(elm);
    */
    if (!myClient) {
      return;
    }

    const piece = pieces[dragInfo.pieceID];
    if (!piece) {
      throw new Error(`Piece ${dragInfo.pieceID} not found`);
    }

    if (sharedHandleDrag(e, piece, dragInfo.offset, r, home, stage)) {
      ref.current?.releasePointerCapture(e.pointerId);
      cancelBlur();
    }
  };
  const handleRotate = (
    e: React.PointerEvent,
    rotateInfo: {pieceID: string; radOffset: number},
  ) => {
    const piece = pieces[rotateInfo.pieceID];
    if (!piece) {
      throw new Error(`Piece ${rotateInfo.pieceID} not found`);
    }

    const pos = coordinateToPosition(piece, home, stage);
    const def = PIECE_DEFINITIONS[parseInt(piece.id)];
    const c = center({
      ...pos,
      width: def.width,
      height: def.height,
    });
    const pointerRads = getAngle(c, {
      x: e.pageX,
      y: e.pageY,
    });

    const newRads = addRadians(pointerRads, -rotateInfo.radOffset);
    const prevRads = piece.handleRotation;
    const newRot = addRadians(piece.rotation, newRads - prevRads);
    r.mutate.updatePiece({
      id: piece.id,
      handleRotation: newRads,
      rotation: newRot,
    });

    if (
      checkSnap(
        {
          ...piece,
          rotation: newRot,
        },
        def,
        pos,
      )
    ) {
      ref.current?.releasePointerCapture(e.pointerId);
    }
  };

  const checkSnap = (
    piece: PieceModel,
    def: PieceDefinition,
    currPos: Position,
  ) => sharedCheckSnap(piece, def, currPos, r, home, stage);

  const handleLostPointerCapture = (e: React.PointerEvent) => {
    dragging.current.delete(e.pointerId);
    rotating.current.delete(e.pointerId);
    isMouseDown.current = false;
    setBodyClass('grabbing', false);
  };

  const handleRotateStart = (
    model: PieceInfo,
    e: React.PointerEvent,
    pos: Position,
  ) => {
    if (!selectIfAvailable(model)) {
      return;
    }

    ref.current?.setPointerCapture(e.pointerId);

    const def = PIECE_DEFINITIONS[parseInt(model.id)];
    const c = center({
      ...pos,
      width: def.width,
      height: def.height,
    });
    const pointerRads = getAngle(c, {
      x: e.pageX,
      y: e.pageY,
    });
    const offset = pointerRads - model.handleRotation;
    rotating.current.set(e.pointerId, {
      pieceID: model.id,
      radOffset: offset,
    });
    setBodyClass('grabbing', true);
  };

  if (!myClient) {
    return null;
  }

  const sizeScale = home.width / SVG_ORIGINAL_SIZE.width;

  return (
    <div
      id="pieces"
      ref={ref}
      onPointerMove={e => handlePointerMove(e)}
      onLostPointerCapture={e => handleLostPointerCapture(e)}
    >
      {Object.values(pieces).map(model => {
        const pos = coordinateToPosition(model, home, stage);
        return (
          <Piece
            key={model.id}
            piece={{
              ...model,
              ...pos,
            }}
            sizeScale={sizeScale}
            selectorID={model.selector}
            myClient={myClient}
            onPointerDown={e => handlePiecePointerDown(model, e, pos)}
            onPointerOver={e => handlePieceHover(model, e)}
            onRotationStart={e => handleRotateStart(model, e, pos)}
          />
        );
      })}
    </div>
  );
}
