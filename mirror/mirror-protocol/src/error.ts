import * as v from 'shared/src/valita.js';
import {baseRequestFields, baseResponseFields} from './base.js';
import {createCall} from './call.js';

export type ErrorInfo = {
  desc: string;
  name?: string | undefined;
  message?: string | undefined;
  stack?: string | undefined;
  cause?: ErrorInfo | undefined;
};

const errorInfoSchema: v.ValitaType<ErrorInfo> = v.lazy<ErrorInfo>(() =>
  v.object({
    desc: v.string(), // String(e)
    name: v.string().optional(), // Error.name
    message: v.string().optional(), // Error.message
    stack: v.string().optional(), // Error.stack
    cause: errorInfoSchema.optional(), // Error.cause
  }),
);

// reflect-cli sends its userParameters, but other agents (e.g. reflect-auth-ui) might send something different.
const agentContextSchema = v.record(v.string());

export const errorReportingRequestSchema = v.object({
  ...baseRequestFields, // userID is empty if it is not known. Importantly, the userAgent is useful.
  action: v.string(), // e.g. "init", "create", "dev", "publish", etc.
  error: errorInfoSchema,
  agentContext: agentContextSchema,
});

export type ErrorReportingRequest = v.Infer<typeof errorReportingRequestSchema>;

export const errorReportingResponseSchema = v.object({
  ...baseResponseFields,
});
export type ErrorReportingResponse = v.Infer<
  typeof errorReportingResponseSchema
>;

export const reportError = createCall(
  'error-report',
  errorReportingRequestSchema,
  errorReportingResponseSchema,
);
