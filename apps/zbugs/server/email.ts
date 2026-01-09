import type {ZbugsRow} from '../shared/zero-hooks.ts';
import {MutationError, MutationErrorCode} from '../shared/error.ts';
import type {ZbugsServerTransaction} from '../shared/mutators.ts';

export async function sendEmail({
  tx,
  email,
  title,
  message,
  link,
  unsubscribeLink,
  issue,
  attachments = [],
}: {
  tx: ZbugsServerTransaction;
  email: string;
  title: string;
  message: string;
  link: string;
  unsubscribeLink: string;
  issue: ZbugsRow['issue'];
  attachments?: {
    filename: string;
    contentType: string;
    data: string; // base64-encoded string
  }[];
}) {
  const apiKey = process.env.LOOPS_EMAIL_API_KEY;
  const transactionalId = process.env.LOOPS_TRANSACTIONAL_ID;
  const idempotencyKey = `${tx.clientID}:${tx.mutationID}:${email}`;

  if (!apiKey || !transactionalId) {
    // oxlint-disable-next-line no-console
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
      unsubscribe: unsubscribeLink,
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
    throw new MutationError(
      `Failed to send Loops email: ${response.status} ${errorText}`,
      MutationErrorCode.NOTIFICATION_FAILED,
    );
  }

  return response.json();
}
