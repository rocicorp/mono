import * as v from 'shared/valita.js';
import {userAgentSchema} from './user-agent.js';

export const baseRequestFields = {
  requester: v.object({
    // UserAgent making the request on behalf of the user. If the version
    // of the user agent is no longer supported, the request will fail
    // with a NOT_SUPPORTED error.
    userAgent: userAgentSchema,

    // The userID on behalf of whom the request is. This is usually the same
    // as the authenticated user (as identified by the Firebase auth token
    // in the CallableContext). However, the authenticated user may instead
    // be user with privileges to act on behalf of the [userID] (e.g. admin).
    userID: v.string(),
  }),
};

export const baseRequestSchema = v.object(baseRequestFields);
export type BaseRequest = v.Infer<typeof baseRequestSchema>;

export const baseResponseFields = {
  success: v.literal(true),
};
export const baseResponseSchema = v.object(baseResponseFields);
export type BaseResponse = v.Infer<typeof baseResponseSchema>;
