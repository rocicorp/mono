import {schema, type Schema} from '../shared/schema.ts';
import {type Transaction, type Row} from '@rocicorp/zero';

export async function sendEmail({
  tx,
  email,
  title,
  message,
  link,
  issue,
  attachments = [],
}: {
  tx: Transaction<Schema>;
  email: string;
  title: string;
  message: string;
  link: string;
  issue: Row<typeof schema.tables.issue>;
  attachments?: {
    filename: string;
    contentType: string;
    data: string; // base64-encoded string
  }[];
}) {
  // Email notifications temporarily disabled
  // See: https://bugs.rocicorp.dev/issue/3877
  return;

  const apiKey = process.env.LOOPS_EMAIL_API_KEY;
  const transactionalId = process.env.LOOPS_TRANSACTIONAL_ID;
  const idempotencyKey = `${tx.clientID}:${tx.mutationID}:${email}`;

  if (!apiKey || !transactionalId) {
    console.log(
      'Missing LOOPS_EMAIL_API_KEY or LOOPS_TRANSACTIONAL_ID Skipping Email',
    );
    return;
  }

  const titleMessage = [title, message].filter(Boolean).join('\n');
  // --- headers for threading ---
  const threadId = `<issue-${issue.id}@bugs.rocicorp.dev>`;
  const messageId = `<${tx.clientID}-${tx.mutationID}-issue-${issue.id}@bugs.rocicorp.dev>`;
  const headers = {
    'Message-ID': messageId,
    'In-Reply-To': threadId,
    'References': threadId,
  };

  const formattedSubject = `#${issue.shortID} ${issue.title.slice(0, 80)}${issue.title.length > 80 ? '...' : ''}`;

  const body = {
    email,
    transactionalId,
    addToAudience: true,
    headers,
    dataVariables: {
      subject: formattedSubject,
      message: titleMessage,
      link,
    },
    attachments,
  };

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
