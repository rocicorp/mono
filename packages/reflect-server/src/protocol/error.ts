import * as s from 'superstruct';

export const errorMessageSchema = s.tuple([s.literal('error'), s.string()]);

export type ErrorMessage = s.Infer<typeof errorMessageSchema>;
