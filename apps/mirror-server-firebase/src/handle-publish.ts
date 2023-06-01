import { NextFunction, Request, Response } from "express";
import multer from "multer";

// Prepare the multer middleware with memory storage
const upload = multer({ storage: multer.memoryStorage() });

export async function handlePublish(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  try {
    upload.single('file')(req, res, function (err: any) {
      if (err instanceof multer.MulterError) {
        // A Multer error occurred when uploading.
        next(Error(`Failed to upload file: ${err.message}`));
      } else if (err) {
        // An unknown error occurred when uploading.
        next(Error(`Failed to upload file: ${err}`));
      }

      // If no errors, file is available in req.file object
      // You can do something with the uploaded file data here.
      console.log(req.file);
      res.status(200).send({ message: "OK", file: req.file.filename });
    })
  } catch (e) {
    next(Error(`Failed to upload file: ${(e as Error).message}`));
  }
}