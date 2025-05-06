import {type Schema} from '../shared/schema.ts';
import {type Transaction} from '@rocicorp/zero';

export async function sendEmail({
  tx,
  email,
  title,
  message,
  link,
  attachments = [],
}: {
  tx: Transaction<Schema>;
  email: string;
  title: string;
  message: string;
  link: string;
  attachments?: {
    filename: string;
    contentType: string;
    data: string; // base64-encoded string
  }[];
}) {
  const apiKey = process.env.LOOPS_EMAIL_API_KEY;
  const transactionalId = process.env.LOOPS_TRANSACTIONAL_ID;

  if (!apiKey || !transactionalId) {
    console.log(
      'Missing LOOPS_EMAIL_API_KEY or LOOPS_TRANSACTIONAL_ID Skipping Email',
    );
    return;
  }

  const body = {
    email,
    transactionalId,
    addToAudience: true,
    dataVariables: {
      subject: title,
      message,
      link,
    },
    attachments,
  };

  const idempotencyKey = `${tx.clientID}:${tx.mutationID}`;
  const options = {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
      'Idempotency-Key': idempotencyKey,
    },
    body: JSON.stringify(body),
  };

  const response = await fetch(
    'https://app.loops.so/api/v1/transactional',
    options,
  );

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(
      `Failed to send Loops email: ${response.status} ${errorText}`,
    );
  }

  return response.json();
}
