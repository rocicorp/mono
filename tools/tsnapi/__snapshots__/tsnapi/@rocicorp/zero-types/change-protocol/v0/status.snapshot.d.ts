// #region Types
export type DownstreamStatus = v.Infer<typeof downstreamStatusSchema>;
export type DownstreamStatusMessage = v.Infer<typeof downstreamStatusMessageSchema>;
export type UpstreamStatusMessage = v.Infer<typeof upstreamStatusMessageSchema>;
// #endregion

// #region Variables
export declare const downstreamStatusMessageSchema: v.TupleType<[v.Type<"status">, v.ObjectType<{
    ack: v.Type<boolean>;
    lagReport: v.Optional<{
        lastTimings: {
            sendTimeMs: number;
            commitTimeMs: number;
            receiveTimeMs: number;
        };
        nextSendTimeMs: number;
    }>;
}, undefined>, v.ObjectType<{
    watermark: v.Type<string>;
}, undefined>]>;
export declare const downstreamStatusSchema: v.ObjectType<{
    ack: v.Type<boolean>;
    lagReport: v.Optional<{
        lastTimings: {
            sendTimeMs: number;
            commitTimeMs: number;
            receiveTimeMs: number;
        };
        nextSendTimeMs: number;
    }>;
}, undefined>;
export declare const upstreamStatusMessageSchema: v.TupleType<[v.Type<"status">, v.UnionType<[v.ObjectType<{
    ack: v.Type<boolean>;
    lagReport: v.Optional<{
        lastTimings: {
            sendTimeMs: number;
            commitTimeMs: number;
            receiveTimeMs: number;
        };
        nextSendTimeMs: number;
    }>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"commit">;
}, undefined>]>, v.ObjectType<{
    watermark: v.Type<string>;
}, undefined>]>;
// #endregion
