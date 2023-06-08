import {useIsomorphicLayoutEffect} from '@/hooks/use-isomorphic-layout-effect';
import type {Reflect} from '@rocicorp/reflect/client';
import type {M} from '../shared/mutators';
import {Cursor} from './Cursor';
import {Rect, Size, positionToCoordinate} from './util';

export function CursorField({
  home,
  stage,
  docSize,
  r,
  clientIDs,
  // TODO(reflect): Make clientID synchronous
  myClientID,
  hideLocalArrow,
  setBodyClass,
}: {
  home: Rect;
  stage: Rect;
  docSize: Size;
  r: Reflect<M>;
  clientIDs: string[];
  myClientID: string;
  hideLocalArrow: boolean;
  setBodyClass: (cls: string, enabled: boolean) => void;
}) {
  useIsomorphicLayoutEffect(() => {
    const handlePointerMove = async (e: PointerEvent) => {
      const coord = positionToCoordinate({x: e.pageX, y: e.pageY}, home, stage);
      const cid = await r.clientID;
      await r.mutate.updateClient({
        id: cid,
        ...coord,
      });
    };
    window.addEventListener('pointermove', handlePointerMove);
    return () => {
      window.removeEventListener('pointermove', handlePointerMove);
    };
  });

  return (
    <div
      id="cursor-field"
      style={{
        height: docSize.height,
      }}
    >
      {[...clientIDs].map(cid => (
        <Cursor
          key={cid}
          r={r}
          clientID={cid}
          isSelf={cid === myClientID}
          hideArrow={cid === myClientID && hideLocalArrow}
          setBodyClass={setBodyClass}
          home={home}
          stage={stage}
        />
      ))}
    </div>
  );
}
