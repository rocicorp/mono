import { useEffect, useState } from "react";
import styles from "./collaborator.module.css";
import type { Replicache } from "replicache";
import { Rect } from "./rect";
import type { M } from "../datamodel/mutators";
import { useClientInfo } from "../datamodel/subscriptions";

const hideCollaboratorDelay = 5000;

interface Position {
  pos: {
    x: number;
    y: number;
  };
  ts: number;
}

export function Collaborator({
  rep,
  clientID,
}: {
  rep: Replicache<M>;
  clientID: string;
}) {
  const clientInfo = useClientInfo(rep, clientID);
  const [lastPos, setLastPos] = useState<Position | null>(null);
  const [gotFirstChange, setGotFirstChange] = useState(false);
  const [, setPoke] = useState({});

  let curPos = null;
  let userInfo = null;
  if (clientInfo) {
    curPos = clientInfo.cursor;
    userInfo = clientInfo.userInfo;
  }

  let elapsed = 0;
  let remaining = 0;
  let visible = false;

  if (curPos) {
    if (!lastPos) {
      console.debug(`Cursor ${clientID} - got initial position`, curPos);
      setLastPos({ pos: curPos, ts: Date.now() });
    } else {
      if (lastPos.pos.x != curPos.x || lastPos.pos.y != curPos.y) {
        console.debug(`Cursor ${clientID} - got change to`, curPos);
        setLastPos({ pos: curPos, ts: Date.now() });
        setGotFirstChange(true);
      }
      if (gotFirstChange) {
        elapsed = Date.now() - lastPos.ts;
        remaining = hideCollaboratorDelay - elapsed;
        visible = remaining > 0;
      }
    }
  }

  useEffect(() => {
    if (remaining > 0) {
      console.debug(`Cursor ${clientID} - setting timer for ${remaining}ms`);
      const timerID = setTimeout(() => setPoke({}), remaining);
      return () => clearTimeout(timerID);
    }
    return;
  });

  console.debug(
    `Cursor ${clientID} - elapsed ${elapsed}, remaining: ${remaining}, visible: ${visible}`
  );
  if (!clientInfo || !curPos || !userInfo) {
    return null;
  }

  return (
    <div className={styles.collaborator} style={{ opacity: visible ? 1 : 0 }}>
      {clientInfo.selectedID && (
        <Rect
          {...{
            rep,
            key: `selection-${clientInfo.selectedID}`,
            id: clientInfo.selectedID,
            highlight: true,
            highlightColor: userInfo.color,
          }}
        />
      )}

      <div
        className={styles.cursor}
        style={{
          left: curPos.x,
          top: curPos.y,
          overflow: "auto",
        }}
      >
        <div className={styles.pointer} style={{ color: userInfo.color }}>
          ➤
        </div>
        <div
          className={styles.userinfo}
          style={{
            backgroundColor: userInfo.color,
            color: "white",
          }}
        >
          {userInfo.avatar}&nbsp;{userInfo.name}
        </div>
      </div>
    </div>
  );
}
