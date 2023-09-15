import {useIsomorphicLayoutEffect} from '@/hooks/use-isomorphic-layout-effect';
import type {Reflect} from '@rocicorp/reflect/client';
import classNames from 'classnames';
import {useSubscribe} from 'replicache-react';
import type {M} from '../shared/mutators';
import {getClient} from './client-model';
import {Rect, coordinateToPosition, simpleHash} from './util';

export function Cursor({
  r,
  clientID,
  home,
  stage,
  isSelf,
  hideArrow,
  setBodyClass,
}: {
  r: Reflect<M>;
  clientID: string;
  home: Rect;
  stage: Rect;
  isSelf: boolean;
  hideArrow: boolean;
  setBodyClass: (cls: string, enabled: boolean) => void;
}) {
  const client = useSubscribe(r, tx => getClient(tx, clientID), undefined);
  const pos = client && coordinateToPosition(client, home, stage);
  const hash = simpleHash(clientID);

  let active: boolean;
  if (!isSelf) {
    // active if a collaborator
    active = client?.focused ?? false;
  } else {
    if ('ontouchstart' in window) {
      // self/touch always inactive
      active = false;
    } else {
      // self/mouse active if in stage
      active =
        pos !== null &&
        pos !== undefined &&
        pos.y >= stage.top() &&
        pos.y <= stage.bottom();
    }
  }

  useIsomorphicLayoutEffect(() => {
    if (isSelf) {
      setBodyClass('custom-cursor', active);
    }
  }, [isSelf, active]);

  if (!client || !pos) {
    return null;
  }

  return (
    <div
      className={classNames('cursor', {active})}
      style={{
        transform: `translate3d(${pos.x}px, ${pos.y}px, 0)`,
      }}
    >
      {!hideArrow && (
        <svg
          version="1.1"
          viewBox="0 0 20 22"
          x="0px"
          y="0px"
          width="20px"
          height="22px"
        >
          <path
            fill={client.color}
            stroke="#fff"
            d="M6.5,16.7l-3.3-16l14.2,8.2L10.5,11c-0.2,0.1-0.4,0.2-0.5,0.4L6.5,16.7z"
          />
        </svg>
      )}
      <div
        className="location"
        style={{
          backgroundColor: client.color,
        }}
      >
        <div className="location-name">
          {client.location ?? `Earth ${['🌎', '🌍', '🌏'][Math.abs(hash % 3)]}`}
        </div>
      </div>
    </div>
  );
}
