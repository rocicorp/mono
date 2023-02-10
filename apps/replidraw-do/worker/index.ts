import {
  consoleLogger,
  DatadogLogger,
  Server as BaseServer,
  TeeLogger,
} from "reps-do";
import { createWorker } from "reps-do";
import { mutators, type M } from "../src/datamodel/mutators.js";

export class Server extends BaseServer<M> {
  constructor(state: DurableObjectState, env: Record<string, string>) {
    let logger = consoleLogger;
    if (env.DATADOG_LOG_LEVEL) {
      logger = new TeeLogger([
        logger,
        new DatadogLogger({
          apiKey: env.DATADOG_API_KEY,
          service: "replidraw",
        }),
      ]);
    }

    super({
      mutators,
      state,
      logger,
    });
  }
}

export default createWorker(async (auth: string, roomID: string) => {
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
});
