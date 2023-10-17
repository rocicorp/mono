import * as v from 'shared/src/valita.js';

const errorKindSchema = v.union(
  v.literal('Unauthorized'),
  v.literal('InvalidConnectionRequest'),
  v.literal('RoomNotFound'),
);

export const errorMessageSchema = v.object({
  type: v.literal('error'),
  kind: errorKindSchema,
  message: v.string(),
});

export type TailErrorKind = v.Infer<typeof errorKindSchema>;

export const connectedMessageSchema = v.object({
  type: v.literal('connected'),
});

const logLevelSchema = v.union(
  v.literal('debug'),
  v.literal('error'),
  v.literal('info'),
  v.literal('log'),
  v.literal('warn'),
);

export type LogLevel = v.Infer<typeof logLevelSchema>;

const logMessageSchema = v.object({
  type: v.literal('log'),
  level: logLevelSchema,
  message: v.array(v.unknown()),
});

export type LogMessage = v.Infer<typeof logMessageSchema>;

export const tailMessageSchema = v.union(
  connectedMessageSchema,
  errorMessageSchema,
  logMessageSchema,
);

export type TailMessage = v.Infer<typeof tailMessageSchema>;
