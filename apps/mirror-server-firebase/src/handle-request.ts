import type Express from "express";
import { handlePublish  } from "./handle-publish.js";

export async function handleRequest(
  req: Express.Request,
  res: Express.Response,
  next: Express.NextFunction,
): Promise<void> {
  if (req.query === undefined) {
    res.status(400).send("Missing query");
    return;
  }

  const { op } = req.params;
  console.log(`Handling request ${JSON.stringify(req.body)}, op: ${op}`);

  switch (op) {
    case "publish":
      return await handlePublish(req, res, next,);
    case "tail":
    case "status":
        res.status(400).send({ error: "Not implemented" });
        break;
    default:
        res.status(400).send({ error: "Invalid op" });
  }

  res.status(400).send({ error: "Invalid op" });
}