import { Rect } from "./rect";
import { DraggableCore, DraggableEvent, DraggableData } from "react-draggable";
import { useShapeByID } from "../datamodel/subscriptions";
import { Replicache } from "replicache";
import { M } from "../datamodel/mutators";

// TODO: In the future I imagine this becoming ShapeController and
// there also be a Shape that wraps Rect and also knows how to draw Circle, etc.
export function RectController({
  rep,
  id,
}: {
  rep: Replicache<M>;
  id: string;
}) {
  const shape = useShapeByID(rep, id);

  const onMouseEnter = async () =>
    rep.mutate.overShape({ clientID: await rep.clientID, shapeID: id });
  const onMouseLeave = async () =>
    rep.mutate.overShape({ clientID: await rep.clientID, shapeID: "" });

  const onDragStart = (e: DraggableEvent, d: DraggableData) => {
    // Can't mark onDragStart async because it changes return type and onDragStart
    // must return void.
    const blech = async () => {
      rep.mutate.selectShape({ clientID: await rep.clientID, shapeID: id });
    };
    blech();
  };
  const onDrag = (e: DraggableEvent, d: DraggableData) => {
    // This is subtle, and worth drawing attention to:
    // In order to properly resolve conflicts, what we want to capture in
    // mutation arguments is the *intent* of the mutation, not the effect.
    // In this case, the intent is the amount the mouse was moved by, locally.
    // We will apply this movement to whatever the state happens to be when we
    // replay. If somebody else was moving the object at the same moment, we'll
    // then end up with a union of the two vectors, which is what we want!
    rep.mutate.moveShape({
      id,
      dx: d.deltaX,
      dy: d.deltaY,
    });
  };

  if (!shape) {
    return null;
  }

  return (
    <DraggableCore onStart={onDragStart} onDrag={onDrag}>
      <div>
        <Rect
          {...{
            rep,
            id,
            highlight: false,
            onMouseEnter,
            onMouseLeave,
          }}
        />
      </div>
    </DraggableCore>
  );
}
