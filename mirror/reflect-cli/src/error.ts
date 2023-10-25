import {ErrorInfo, Severity, reportError} from 'mirror-protocol/src/error.js';
import type {ArgumentsCamelCase} from 'yargs';
import {getAuthentication} from './auth-config.js';
import {getUserParameters} from './metrics/send-ga-event.js';
import {version} from './version.js';
import type {CommonYargsOptions} from './yarg-types.js';

export class ErrorWrapper extends Error {
  readonly severity: Severity;

  constructor(e: unknown, severity: Severity) {
    super(e instanceof Error ? e.message : String(e), {cause: e});
    this.severity = severity;
  }
}

export async function reportE(
  args: ArgumentsCamelCase<CommonYargsOptions>,
  eventName: string,
  e: unknown,
  severity?: Severity,
) {
  let userID = '';
  try {
    ({userID} = await getAuthentication(args));
  } catch (e) {
    /* swallow */
  }

  severity ??= e instanceof ErrorWrapper ? e.severity : 'ERROR';
  await reportError({
    action: eventName,
    error: createErrorInfo(e),
    severity,
    requester: {
      userID,
      userAgent: {type: 'reflect-cli', version},
    },
    agentContext: getUserParameters(version),
  }).catch(_err => {
    /* swallow */
  });
}

function createErrorInfo(e: unknown): ErrorInfo {
  e = e instanceof ErrorWrapper ? e.cause : e;
  if (!(e instanceof Error)) {
    return {desc: String(e)};
  }
  return {
    desc: String(e),
    name: e.name,
    message: e.message,
    stack: e.stack,
    cause: createErrorInfo(e.cause),
  };
}
