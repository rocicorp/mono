import { useEffect, useState } from "react";
import { Reflect } from "@rocicorp/reflect";
import { Designer } from "../../frontend/designer";
import { Nav } from "../../frontend/nav";
import { M, clientMutators } from "../../datamodel/mutators";
import { randUserInfo } from "../../datamodel/client-state";
import { nanoid } from "nanoid";
import { nodeConsoleLogSink, OptionalLoggerImpl } from "@rocicorp/logger";
import { DataDogBrowserLogSink } from "../../frontend/data-dog-browser-log-sink";
import { workerWsURI, workerURL } from "../../util/host";
import { Metrics, Reporter } from "@rocicorp/datadog-util";

export default function Home() {
  const [reflect, setReflectClient] = useState<Reflect<M> | null>(null);
  const [online, setOnline] = useState(false);

  const logSink = nodeConsoleLogSink;
  const logger = new OptionalLoggerImpl(logSink);
  const logSinks = [logSink];
  if (process.env.NEXT_PUBLIC_DATADOG_CLIENT_TOKEN !== undefined) {
    logSinks.push(new DataDogBrowserLogSink());
  }

  useEffect(() => {
    const [, , roomID] = location.pathname.split("/");

    (async () => {
      logger.info?.(`Connecting to worker at ${workerWsURI}`);
      const userID = nanoid();
      const metrics = new Metrics();
      // TODO figure out why we can't use ROUTES.reportMetrics here from reflect-server.
      const metricsEndpoint = new URL("/api/metrics/v0/report", workerURL);
      new Reporter({
        metrics,
        url: metricsEndpoint.toString(),
      });

      const r = new Reflect<M>({
        socketOrigin: workerWsURI,
        onOnlineChange: setOnline,
        userID,
        roomID,
        auth: JSON.stringify({
          userID,
          roomID,
        }),
        logSinks,
        mutators: clientMutators,
        metrics,
        buffer: 250,
        // maxRandomPushLatency: 100,
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
