import { NextFunction, Request, Response } from "express";
import busboy from "busboy";

// Prepare the multer middleware with memory storage

export async function handlePublish(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    const bb = busboy({
      headers: req.headers,
    });

    await new Promise((resolve, reject) => {
      bb.once('finish', resolve)
        .once('error', reject)
        .on('file', (fieldname, file, info) => {
          file.resume();
          file.pipe(process.stdout);
          console.dir({ fieldname, file, info }, { depth: null });
        })
        .end(req.body);
    });

    res.sendStatus(200);
  } catch (error) {
    next(error);
  }
}