import {PutObjectCommand, S3Client} from '@aws-sdk/client-s3';
import {getSignedUrl} from '@aws-sdk/s3-request-presigner';
import {nanoid} from 'nanoid';
import {ALLOWED_CONTENT_TYPES} from '../../shared/consts.ts';

if (!process.env.AWS_REGION) {
  // oxlint-disable-next-line no-console -- Configuration warning in demo app
  console.warn('AWS_REGION is not set');
}
if (!process.env.AWS_ACCESS_KEY_ID) {
  // oxlint-disable-next-line no-console -- Configuration warning in demo app
  console.warn('AWS_ACCESS_KEY_ID is not set');
}
if (!process.env.AWS_SECRET_ACCESS_KEY) {
  // oxlint-disable-next-line no-console -- Configuration warning in demo app
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

  if (!ALLOWED_CONTENT_TYPES.includes(contentType)) {
    throw new Error(
      `Invalid content type: ${contentType}. Allowed types: ${ALLOWED_CONTENT_TYPES.join(', ')}`,
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
