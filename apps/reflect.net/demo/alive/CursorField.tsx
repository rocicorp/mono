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
  presentClientIDs: clientIDs,
  botIDs,
  myClientID,
  hideLocalArrow,
  setBodyClass,
}: {
  home: Rect;
  stage: Rect;
  docSize: Size;
  r: Reflect<M>;
  presentClientIDs: ReadonlySet<string>;
  botIDs: string[];
  myClientID: string;
  hideLocalArrow: boolean;
  setBodyClass: (cls: string, enabled: boolean) => void;
}) {
  useIsomorphicLayoutEffect(() => {
    const handlePointerMove = async (e: PointerEvent) => {
      const coord = positionToCoordinate({x: e.pageX, y: e.pageY}, home, stage);
      await r.mutate.updateClient({
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
          key={`c-${cid}`}
          r={r}
          id={cid}
          type="client"
          isSelf={cid === myClientID}
          hideArrow={cid === myClientID && hideLocalArrow}
          setBodyClass={setBodyClass}
          home={home}
          stage={stage}
        />
      ))}
      {[...botIDs].map(bid => (
        <Cursor
          key={`b-${bid}`}
          r={r}
          id={bid}
          type="bot"
          isSelf={false}
          hideArrow={false}
          setBodyClass={setBodyClass}
          home={home}
          stage={stage}
        />
      ))}
    </div>
  );
}
