import busboy from 'busboy';
import * as functions from 'firebase-functions';

/**
 * Publish function.
 * NOTE: This function will probably not use a multi/part form in the future and just handle a standard JSON payload.
 */
export async function publish(
  req: functions.Request,
  res: functions.Response,
): Promise<void> {
  try {
    const bb = busboy({
      headers: req.headers,
    });

    const bundle: Buffer[] = [];
    const sourcemap: Buffer[] = [];
    const files = await new Promise<Buffer[]>((resolve, reject) => {
      bb.once('finish', () => {
        const bundleBuffer = Buffer.concat(bundle);
        const sourcemapBuffer = Buffer.concat(sourcemap);
        resolve([bundleBuffer, sourcemapBuffer]);
      })
        .once('error', reject)
        .on('file', async (fieldname, file) => {
          const chunks: Uint8Array[] = [];
          file.on('data', data => chunks.push(data));
          file.on('end', () => {
            if (fieldname === 'bundle') {
              bundle.push(Buffer.concat(chunks));
            } else if (fieldname === 'sourcemap') {
              sourcemap.push(Buffer.concat(chunks));
            }
          });
        })
        .end(req.body);
    });
    const bundleFile = files[0];
    const sourceMapFile = files[1];
    console.log('bundleFile', bundleFile.toString('utf8'));
    console.log('sourceMapFile', sourceMapFile.toString('utf8'));
    console.log("Now calling Erik's code to publish!!");
    res.sendStatus(200);
  } catch (error) {
    res.sendStatus(500);
  }
}
