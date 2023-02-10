import type { Reflect } from "@rocicorp/reflect";
import { DraggableCore, DraggableEvent, DraggableData } from "react-draggable";
import { Rect } from "./rect";
import type { M } from "../datamodel/mutators";
import { useShapeByID } from "../datamodel/subscriptions";

export function Selection({
  reflect,
  id,
  containerOffsetTop,
}: {
  reflect: Reflect<M>;
  id: string;
  containerOffsetTop: number | null;
}) {
  const shape = useShapeByID(reflect, id);
  const gripSize = 19;

  if (!shape) {
    return null;
  }

  const { x, y, width, height, rotate } = shape;

  const center = () => {
    return {
      x: x + width / 2,
      y: y + height / 2,
    };
  };

  const onResize = (_e: DraggableEvent, d: DraggableData) => {
    const shapeCenter = center();

    const size = (x1: number, x2: number, y1: number, y2: number) => {
      const distanceSqFromCenterToCursor =
        Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2);
      return Math.sqrt(distanceSqFromCenterToCursor / 2) * 2;
    };

    const s0 = size(
      shapeCenter.x,
      d.x - d.deltaX,
      shapeCenter.y,
      d.y - d.deltaY
    );
    const s1 = size(shapeCenter.x, d.x, shapeCenter.y, d.y);

    reflect.mutate.resizeShape({ id, ds: s1 - s0 });
  };

  const onRotate = (_e: DraggableEvent, d: DraggableData) => {
    if (containerOffsetTop === null) {
      return;
    }

    const offsetY = d.y - containerOffsetTop;

    const shapeCenter = center();
    const before = Math.atan2(
      offsetY - d.deltaY - shapeCenter.y,
      d.x - d.deltaX - shapeCenter.x
    );
    const after = Math.atan2(offsetY - shapeCenter.y, d.x - shapeCenter.x);

    reflect.mutate.rotateShape({
      id,
      ddeg: ((after - before) * 180) / Math.PI,
    });
  };

  return (
    <div>
      <Rect
        {...{
          reflect,
          id,
          highlight: true,
        }}
      />
      <div
        style={{
          position: "absolute",
          transform: `translate3d(${x}px, ${y}px, 0) rotate(${rotate}deg)`,
          width,
          height,
          pointerEvents: "none",
        }}
      >
        <DraggableCore onDrag={onResize}>
          <svg
            width={gripSize}
            height={gripSize}
            style={{
              position: "absolute",
              transform: `translate3d(${width - gripSize / 2 - 2}px, ${
                height - gripSize / 2 - 2
              }px, 0)`,
              cursor: "grab",
              pointerEvents: "all",
            }}
          >
            <rect
              strokeWidth={2}
              stroke="rgb(74,158,255)"
              width={gripSize}
              height={gripSize}
              fill="white"
            />
          </svg>
        </DraggableCore>
        <DraggableCore onDrag={onRotate}>
          <svg
            width={gripSize}
            height={gripSize}
            style={{
              position: "absolute",
              transform: `translate3d(${width + gripSize * 1.5}px, ${
                height / 2 - gripSize / 2
              }px, 0)`,
              cursor: "grab",
              pointerEvents: "all",
            }}
          >
            <ellipse
              cx={gripSize / 2}
              cy={gripSize / 2}
              rx={gripSize / 2 - 1}
              ry={gripSize / 2 - 1}
              strokeWidth={2}
              stroke="rgb(74,158,255)"
              fill="white"
            />
          </svg>
        </DraggableCore>
      </div>
    </div>
  );
}
