import { useEffect, useState } from "react";
import { Reflect } from "@rocicorp/reflect";
import { Designer } from "../../frontend/designer";
import { Nav } from "../../frontend/nav";
import { M, clientMutators } from "../../datamodel/mutators";
import { randUserInfo } from "../../datamodel/client-state";
import { consoleLogSink, OptionalLoggerImpl } from "@rocicorp/logger";
import { DataDogBrowserLogSink } from "../../frontend/data-dog-browser-log-sink";
import { workerWsURI } from "../../util/host";

// used to change mutators to test creating new client groups
// and mutation recovery
type M2 = M & {
  blah: () => Promise<void>;
};

export default function Home() {
  const [reflect, setReflectClient] = useState<Reflect<M2> | null>(null);
  const [online, setOnline] = useState(false);

  const logSink = process.env.NEXT_PUBLIC_DATADOG_CLIENT_TOKEN
    ? new DataDogBrowserLogSink()
    : consoleLogSink;
  const logger = new OptionalLoggerImpl(logSink);

  useEffect(() => {
    const [, , roomID] = location.pathname.split("/");

    (async () => {
      logger.info?.(`Connecting to worker at ${workerWsURI}`);
      const userID = "test-user";
      const r = new Reflect<M2>({
        socketOrigin: workerWsURI,
        onOnlineChange: setOnline,
        userID,
        roomID,
        auth: JSON.stringify({
          userID,
          roomID,
        }),
        logSinks: [logSink],
        mutators: {
          ...clientMutators,
          blah: async () => {},
        },
      });

      const defaultUserInfo = randUserInfo();
      await r.mutate.initClientState({
        id: await r.clientID,
        defaultUserInfo,
      });
      await r.mutate.initShapes();

      setReflectClient(r);
    })();
  }, []);

  if (!reflect) {
    return null;
  }

  return (
    <div
      style={{
        position: "absolute",
        display: "flex",
        flexDirection: "column",
        left: 0,
        top: 0,
        width: "100%",
        height: "100%",
        background: "rgb(229,229,229)",
      }}
    >
      <Nav reflect={reflect} online={online} />
      <Designer reflect={reflect} logger={logger} />
    </div>
  );
}
