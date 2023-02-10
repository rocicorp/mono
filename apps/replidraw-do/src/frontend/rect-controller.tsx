import type { Reflect } from "@rocicorp/reflect";
import { DraggableCore, DraggableEvent, DraggableData } from "react-draggable";
import { Rect } from "./rect";
import type { M } from "../datamodel/mutators";
import type { UndoManager } from "@rocicorp/undo";
import React from "react";

// TODO: In the future I imagine this becoming ShapeController and
// there also be a Shape that wraps Rect and also knows how to draw Circle, etc.
export function RectController({
  reflect,
  id,
  undoManager,
}: {
  reflect: Reflect<M>;
  id: string;
  undoManager: UndoManager;
}) {
  const onMouseEnter = async () =>
    reflect.mutate.overShape({
      clientID: await reflect.clientID,
      shapeID: id,
    });
  const onMouseLeave = async () =>
    reflect.mutate.overShape({
      clientID: await reflect.clientID,
      shapeID: "",
    });

  const onDragStart = (_e: DraggableEvent, _d: DraggableData) => {
    // Can't mark onDragStart async because it changes return type and onDragStart
    // must return void.
    undoManager.startGroup();
    const blech = async () => {
      reflect.mutate.selectShape({
        clientID: await reflect.clientID,
        shapeID: id,
      });
    };
    blech();
  };

  const onDrag = (_e: DraggableEvent, d: DraggableData) => {
    // This is subtle, and worth drawing attention to:
    // In order to properly resolve conflicts, what we want to capture in
    // mutation arguments is the *intent* of the mutation, not the effect.
    // In this case, the intent is the amount the mouse was moved by, locally.
    // We will apply this movement to whatever the state happens to be when we
    // replay. If somebody else was moving the object at the same moment, we'll
    // then end up with a union of the two vectors, which is what we want!

    undoManager.add({
      undo: () => {
        return reflect.mutate.moveShape({
          id,
          dx: -d.deltaX,
          dy: -d.deltaY,
        });
      },
      execute: () => {
        return reflect.mutate.moveShape({
          id,
          dx: d.deltaX,
          dy: d.deltaY,
        });
      },
    });
  };

  const onDragStop = (_e: DraggableEvent, _d: DraggableData) => {
    undoManager.endGroup();
  };

  return (
    <DraggableCore onStart={onDragStart} onDrag={onDrag} onStop={onDragStop}>
      <div>
        <Rect
          {...{
            reflect,
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
