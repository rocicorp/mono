import {
  consoleLogger,
  DatadogLogger,
  Logger,
  Server as BaseServer,
  TeeLogger,
} from "reflect";
import { createWorker, BaseWorkerEnv } from "reflect";
import { mutators, type M } from "../src/datamodel/mutators.js";

function createLogger(service: string, datadogApiKey?: string): Logger {
  let logger = consoleLogger;
  if (datadogApiKey) {
    logger = new TeeLogger([
      logger,
      new DatadogLogger({
        apiKey: datadogApiKey,
        service,
      }),
    ]);
  }
  return logger;
}

export class Server extends BaseServer<M> {
  constructor(state: DurableObjectState, env: Record<string, string>) {
    super({
      mutators,
      state,
      logger: createLogger("replidraw-do", env.DATADOG_API_KEY),
      logLevel: "info",
    });
  }
}

interface WorkerEnv extends BaseWorkerEnv {
  DATADOG_API_KEY?: string;
}

export default createWorker<WorkerEnv>({
  authHandler: async (auth: string, roomID: string) => {
    // Note a real implementation should use signed and encrypted auth tokens,
    // or store the auth tokens in a session database for validation.
    const authJson = JSON.parse(auth);
    if (!authJson) {
      throw Error("Empty auth");
    }
    if (authJson.roomID !== roomID) {
      throw new Error("incorrect roomID");
    }
    if (!authJson.userID || typeof authJson.userID !== "string") {
      throw new Error("Missing userID");
    }
    return {
      userID: authJson.userID,
    };
  },
  createLogger(env: WorkerEnv) {
    return createLogger("replidraw-worker", env.DATADOG_API_KEY);
  },
  getLogLevel(_env: WorkerEnv) {
    return "info";
  },
});
