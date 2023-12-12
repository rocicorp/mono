import {ErrorInfo, Severity, reportError} from 'mirror-protocol/src/error.js';
import type {ArgumentsCamelCase} from 'yargs';
import {getAuthentication} from './auth-config.js';
import {getUserParameters} from './metrics/send-ga-event.js';
import {version} from './version.js';
import type {CommonYargsOptions} from './yarg-types.js';

type ReportSeverity = Severity | 'DO_NOT_REPORT';

export class ErrorWithSeverity extends Error {
  readonly severity: ReportSeverity;

  constructor(
    message: string,
    severity: ReportSeverity,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.severity = severity;
  }
}

export class UserError extends ErrorWithSeverity {
  constructor(message: string) {
    super(message, 'DO_NOT_REPORT');
  }
}

export class ErrorWrapper extends ErrorWithSeverity {
  constructor(e: unknown, severity: ReportSeverity) {
    super(e instanceof Error ? e.message : String(e), severity, {cause: e});
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

  if (e instanceof ErrorWithSeverity) {
    if (e.severity === 'DO_NOT_REPORT') {
      return;
    }
    severity ??= e.severity;
  } else {
    severity ??= 'ERROR';
  }
  const error = {
    action: eventName,
    error: createErrorInfo(e),
    severity,
    requester: {
      userID,
      userAgent: {type: 'reflect-cli', version},
    },
    agentContext: getUserParameters(version, userID),
  };
  // console.debug(error); // For testing
  await reportError(error).catch(_err => {
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
