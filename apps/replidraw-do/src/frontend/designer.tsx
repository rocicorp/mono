import type { Reflect } from "@rocicorp/reflect";
import type { OptionalLogger } from "@rocicorp/logger";
import React, { useRef, useState } from "react";
import { HotKeys } from "react-hotkeys";
import { DraggableCore } from "react-draggable";
import { Rect } from "./rect";
import { Collaborator } from "./collaborator";
import { RectController } from "./rect-controller";
import { touchToMouse } from "./events";
import { Selection } from "./selection";
import {
  useShapeIDs,
  useOverShapeID,
  useSelectedShapeID,
  useCollaboratorIDs,
} from "../datamodel/subscriptions";
import type { M } from "../datamodel/mutators";
import type { UndoManager } from "@rocicorp/undo";
import { getShape } from "../datamodel/shape";

export function Designer({
  reflect,
  logger,
  undoManager,
}: {
  reflect: Reflect<M>;
  logger: OptionalLogger;
  undoManager: UndoManager;
}) {
  const ids = useShapeIDs(reflect);
  const overID = useOverShapeID(reflect);
  const selectedID = useSelectedShapeID(reflect);
  const collaboratorIDs = useCollaboratorIDs(reflect);

  const ref = useRef<HTMLDivElement | null>(null);
  const [dragging, setDragging] = useState(false);

  const move = async (dx: number = 0, dy: number = 0) => {
    await reflect.mutate.moveShape({ id: selectedID, dx, dy });
  };

  const handlers = {
    moveLeft: () => {
      undoManager.add({
        execute: () => move(-20, 0),
        undo: () => move(20, 0),
      });
    },
    moveRight: () => {
      undoManager.add({
        execute: () => move(20, 0),
        undo: () => move(-20, 0),
      });
    },
    moveUp: () => {
      undoManager.add({
        execute: () => move(0, -20),
        undo: () => move(0, 20),
      });
    },
    moveDown: () => {
      undoManager.add({
        execute: () => move(0, 20),
        undo: () => move(0, -20),
      });
    },
    deleteShape: async () => {
      // Prevent navigating backward on some browsers.
      event?.preventDefault();
      const shapeBeforeDelete = await reflect.query((tx) =>
        getShape(tx, selectedID)
      );
      // //no-op non-exisitent shape on delete
      if (!shapeBeforeDelete) {
        return;
      }
      const deleteShape = async () =>
        await reflect.mutate.deleteShape(selectedID);
      const createShape = async () => {
        await reflect.mutate.createShape({
          id: selectedID,
          shape: shapeBeforeDelete,
        });
      };
      undoManager.add({
        execute: deleteShape,
        undo: createShape,
      });
    },
    undo: async () => {
      await undoManager.undo();
    },
    redo: async () => {
      await undoManager.redo();
    },
  };

  const onMouseMove = async ({
    pageX,
    pageY,
  }: {
    pageX: number;
    pageY: number;
  }) => {
    if (ref && ref.current) {
      reflect.mutate.setCursor({
        id: await reflect.clientID,
        x: pageX,
        y: pageY - ref.current.offsetTop,
      });
    }
  };

  return (
    <HotKeys
      {...{
        style: { outline: "none", display: "flex", flex: 1 },
        keyMap,
        handlers,
      }}
    >
      <DraggableCore
        onStart={() => setDragging(true)}
        onStop={() => setDragging(false)}
      >
        <div
          {...{
            ref,
            style: {
              position: "relative",
              display: "flex",
              flex: 1,
              overflow: "hidden",
            },
            onMouseMove,
            onTouchMove: (e) => touchToMouse(e, onMouseMove),
          }}
        >
          {ids.map((id) => (
            // draggable rects
            <RectController
              {...{
                key: `shape-${id}`,
                reflect,
                id,
                undoManager,
              }}
            />
          ))}

          {
            // self-highlight
            !dragging && overID && (
              <Rect
                {...{
                  key: `highlight-${overID}`,
                  reflect,
                  id: overID,
                  highlight: true,
                }}
              />
            )
          }

          {
            // self-selection
            selectedID && (
              <Selection
                {...{
                  key: `selection-${selectedID}`,
                  reflect,
                  id: selectedID,
                  highlight: true,
                  containerOffsetTop: ref.current && ref.current.offsetTop,
                  undoManager,
                }}
              />
            )
          }

          {
            // collaborators
            // foreignObject seems super buggy in Safari, so instead we do the
            // text labels in an HTML context, then do collaborator selection
            // rectangles as their own independent svg content. Le. Sigh.
            collaboratorIDs.map((id) => (
              <Collaborator
                key={`key-${id}`}
                reflect={reflect}
                clientID={id}
                logger={logger}
              />
            ))
          }
        </div>
      </DraggableCore>
    </HotKeys>
  );
}

const keyMap = {
  moveLeft: ["left", "shift+left"],
  moveRight: ["right", "shift+right"],
  moveUp: ["up", "shift+up"],
  moveDown: ["down", "shift+down"],
  deleteShape: ["del", "backspace"],
  undo: ["ctrl+z", "command+z"],
  redo: ["ctrl+y", "command+shift+z"],
};
