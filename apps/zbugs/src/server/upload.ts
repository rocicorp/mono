import {S3Client, PutObjectCommand} from '@aws-sdk/client-s3';
import {getSignedUrl} from '@aws-sdk/s3-request-presigner';
import {nanoid} from 'nanoid';

if (!process.env.AWS_REGION) {
  // eslint-disable-next-line no-console -- Configuration warning in demo app
  console.warn('AWS_REGION is not set');
}
if (!process.env.AWS_ACCESS_KEY_ID) {
  // eslint-disable-next-line no-console -- Configuration warning in demo app
  console.warn('AWS_ACCESS_KEY_ID is not set');
}
if (!process.env.AWS_SECRET_ACCESS_KEY) {
  // eslint-disable-next-line no-console -- Configuration warning in demo app
  console.warn('AWS_SECRET_ACCESS_KEY is not set');
}

const s3 =
  process.env.AWS_REGION &&
  process.env.AWS_ACCESS_KEY_ID &&
  process.env.AWS_SECRET_ACCESS_KEY
    ? new S3Client({
        region: process.env.AWS_REGION,
        credentials: {
          accessKeyId: process.env.AWS_ACCESS_KEY_ID,
          secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        },
      })
    : undefined;

const BUCKET_NAME = 'zbugs-image-uploads';

export async function getPresignedUrl(
  contentType: string,
): Promise<{url: string; key: string}> {
  if (!s3) {
    throw new Error(
      'S3 client is not initialized due to missing environment variables',
    );
  }

  const key = nanoid();
  const command = new PutObjectCommand({
    Bucket: BUCKET_NAME,
    Key: key,
    ContentType: contentType,
  });

  const url = await getSignedUrl(s3, command, {expiresIn: 3600});
  return {url, key};
}
