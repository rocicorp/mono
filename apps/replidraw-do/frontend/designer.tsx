import React, { useRef, useState } from "react";
import { Rect } from "./rect";
import { HotKeys } from "react-hotkeys";
import { Collaborator } from "./collaborator";
import { RectController } from "./rect-controller";
import { touchToMouse } from "./events";
import { Selection } from "./selection";
import { DraggableCore } from "react-draggable";
import {
  useShapeIDs,
  useOverShapeID,
  useSelectedShapeID,
  useCollaboratorIDs,
} from "../datamodel/subscriptions";
import { Replicache } from "replicache";
import { M } from "../datamodel/mutators";

export function Designer({ rep }: { rep: Replicache<M> }) {
  const ids = useShapeIDs(rep);
  const overID = useOverShapeID(rep);
  const selectedID = useSelectedShapeID(rep);
  const collaboratorIDs = useCollaboratorIDs(rep);

  const ref = useRef<HTMLDivElement | null>(null);
  const [dragging, setDragging] = useState(false);

  const handlers = {
    moveLeft: () => rep.mutate.moveShape({ id: selectedID, dx: -20, dy: 0 }),
    moveRight: () => rep.mutate.moveShape({ id: selectedID, dx: 20, dy: 0 }),
    moveUp: () => rep.mutate.moveShape({ id: selectedID, dx: 0, dy: -20 }),
    moveDown: () => rep.mutate.moveShape({ id: selectedID, dx: 0, dy: 20 }),
    deleteShape: () => {
      // Prevent navigating backward on some browsers.
      event?.preventDefault();
      rep.mutate.deleteShape(selectedID);
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
      rep.mutate.setCursor({
        id: await rep.clientID,
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
                rep,
                id,
              }}
            />
          ))}

          {
            // self-highlight
            !dragging && overID && (
              <Rect
                {...{
                  key: `highlight-${overID}`,
                  rep,
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
                  rep,
                  id: selectedID,
                  highlight: true,
                  containerOffsetTop: ref.current && ref.current.offsetTop,
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
                {...{
                  key: `key-${id}`,
                  rep,
                  clientID: id,
                }}
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
};
