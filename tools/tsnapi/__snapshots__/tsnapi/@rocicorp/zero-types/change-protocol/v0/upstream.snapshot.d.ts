// #region Types
export type BackfillRequest = v.Infer<typeof backfillRequestSchema>;
export type ChangeSourceUpstream = v.Infer<typeof changeSourceUpstreamSchema>;
// #endregion

// #region Variables
export declare const backfillRequestSchema: v.ObjectType<{
    table: v.ObjectType<Omit<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, "metadata"> & {
        metadata: v.Type<{
            [x: string]: import("../../../../../../shared/src/bigint-json.ts").JSONValue;
            rowKey: Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>;
        } | null>;
    }, undefined>;
    columns: v.Type<Record<string, Record<string, import("./json.ts").JSONValue | undefined>>>;
}, undefined>;
export declare const changeSourceUpstreamSchema: v.TupleType<[v.Type<"status">, v.UnionType<[v.ObjectType<{
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
