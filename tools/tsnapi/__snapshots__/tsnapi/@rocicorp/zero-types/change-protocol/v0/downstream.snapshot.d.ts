// #region Types
export type Begin = v.Infer<typeof begin>;
export type ChangeStreamControl = v.Infer<typeof changeStreamControlSchema>;
export type ChangeStreamData = v.Infer<typeof changeStreamDataSchema>;
export type ChangeStreamMessage = v.Infer<typeof changeStreamMessageSchema>;
export type Commit = v.Infer<typeof commit>;
export type Data = v.Infer<typeof data>;
export type Rollback = v.Infer<typeof rollback>;
// #endregion

// #region Variables
export declare const changeStreamControlSchema: v.TupleType<[v.Type<"control">, v.ObjectType<{
    tag: v.Type<"reset-required">;
    message: v.Optional<string>;
    errorDetails: v.Optional<Record<string, import("./json.ts").JSONValue | undefined>>;
}, undefined>]>;
export declare const changeStreamDataSchema: v.UnionType<[v.TupleType<[v.Type<"begin">, v.ObjectType<{
    tag: v.Type<"begin">;
    json: v.Optional<"p" | "s">;
    skipAck: v.Optional<boolean>;
}, undefined>, v.ObjectType<{
    commitWatermark: v.Type<string>;
}, undefined>]>, v.TupleType<[v.Type<"data">, v.UnionType<[v.UnionType<[v.ObjectType<{
    tag: v.Type<"insert">;
    relation: v.Type<{
        rowKey: {
            type?: "default" | "nothing" | "full" | "index" | undefined;
            columns: string[];
        };
        keyColumns?: string[] | undefined;
        replicaIdentity?: "default" | "nothing" | "full" | "index" | undefined;
        schema: string;
        name: string;
    }>;
    new: v.Type<Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"update">;
    relation: v.Type<{
        rowKey: {
            type?: "default" | "nothing" | "full" | "index" | undefined;
            columns: string[];
        };
        keyColumns?: string[] | undefined;
        replicaIdentity?: "default" | "nothing" | "full" | "index" | undefined;
        schema: string;
        name: string;
    }>;
    key: v.Type<Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue> | null>;
    new: v.Type<Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"delete">;
    relation: v.Type<{
        rowKey: {
            type?: "default" | "nothing" | "full" | "index" | undefined;
            columns: string[];
        };
        keyColumns?: string[] | undefined;
        replicaIdentity?: "default" | "nothing" | "full" | "index" | undefined;
        schema: string;
        name: string;
    }>;
    key: v.Type<Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"truncate">;
    relations: v.ArrayType<v.Type<{
        rowKey: {
            type?: "default" | "nothing" | "full" | "index" | undefined;
            columns: string[];
        };
        keyColumns?: string[] | undefined;
        replicaIdentity?: "default" | "nothing" | "full" | "index" | undefined;
        schema: string;
        name: string;
    }>>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"backfill">;
    relation: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
        rowKey: v.ObjectType<{
            columns: v.ArrayType<v.Type<string>>;
            type: v.Optional<"default" | "nothing" | "full" | "index">;
        }, undefined>;
    }, undefined>;
    columns: v.ArrayType<v.Type<string>>;
    watermark: v.Type<string>;
    rowValues: v.ArrayType<v.ArrayType<v.Type<import("../../../../../../shared/src/bigint-json.ts").JSONValue>>>;
    status: v.Optional<{
        totalBytes?: number | undefined;
        rows: number;
        totalRows: number;
    }>;
}, undefined>]>, v.UnionType<[v.ObjectType<{
    tag: v.Type<"create-table">;
    spec: v.ObjectType<Omit<{
        name: v.Type<string>;
        columns: v.Type<Record<string, {
            pgTypeClass?: "e" | "d" | "b" | "c" | "p" | "r" | "m" | undefined;
            elemPgTypeClass?: "e" | "d" | "b" | "c" | "p" | "r" | "m" | null | undefined;
            characterMaximumLength?: number | null | undefined;
            notNull?: boolean | null | undefined;
            dflt?: string | null | undefined;
            pos: number;
            dataType: string;
        }>>;
        primaryKey: v.Optional<string[]>;
    }, "schema"> & {
        schema: v.Type<string>;
    }, undefined>;
    metadata: v.Optional<{
        [x: string]: import("../../../../../../shared/src/bigint-json.ts").JSONValue;
        rowKey: Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>;
    }>;
    backfill: v.Optional<Record<string, Record<string, import("./json.ts").JSONValue | undefined>>>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"rename-table">;
    old: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
    new: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"update-table-metadata">;
    table: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
    old: v.ObjectType<{
        rowKey: v.Type<Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
    }, v.Type<import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
    new: v.ObjectType<{
        rowKey: v.Type<Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
    }, v.Type<import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"add-column">;
    table: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
    column: v.ObjectType<{
        name: v.Type<string>;
        spec: v.ObjectType<{
            pos: v.Type<number>;
            dataType: v.Type<string>;
            pgTypeClass: v.Optional<"e" | "d" | "b" | "c" | "p" | "r" | "m">;
            elemPgTypeClass: v.Optional<"e" | "d" | "b" | "c" | "p" | "r" | "m" | null>;
            characterMaximumLength: v.Optional<number | null>;
            notNull: v.Optional<boolean | null>;
            dflt: v.Optional<string | null>;
        }, undefined>;
    }, undefined>;
    tableMetadata: v.Optional<{
        [x: string]: import("../../../../../../shared/src/bigint-json.ts").JSONValue;
        rowKey: Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>;
    }>;
    backfill: v.Optional<Record<string, import("./json.ts").JSONValue | undefined>>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"update-column">;
    table: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
    old: v.ObjectType<{
        name: v.Type<string>;
        spec: v.ObjectType<{
            pos: v.Type<number>;
            dataType: v.Type<string>;
            pgTypeClass: v.Optional<"e" | "d" | "b" | "c" | "p" | "r" | "m">;
            elemPgTypeClass: v.Optional<"e" | "d" | "b" | "c" | "p" | "r" | "m" | null>;
            characterMaximumLength: v.Optional<number | null>;
            notNull: v.Optional<boolean | null>;
            dflt: v.Optional<string | null>;
        }, undefined>;
    }, undefined>;
    new: v.ObjectType<{
        name: v.Type<string>;
        spec: v.ObjectType<{
            pos: v.Type<number>;
            dataType: v.Type<string>;
            pgTypeClass: v.Optional<"e" | "d" | "b" | "c" | "p" | "r" | "m">;
            elemPgTypeClass: v.Optional<"e" | "d" | "b" | "c" | "p" | "r" | "m" | null>;
            characterMaximumLength: v.Optional<number | null>;
            notNull: v.Optional<boolean | null>;
            dflt: v.Optional<string | null>;
        }, undefined>;
    }, undefined>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"drop-column">;
    table: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
    column: v.Type<string>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"drop-table">;
    id: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"create-index">;
    spec: v.ObjectType<Omit<{
        name: v.Type<string>;
        tableName: v.Type<string>;
        unique: v.Type<boolean>;
        columns: v.Type<Record<string, "ASC" | "DESC">>;
    }, "schema"> & {
        schema: v.Type<string>;
    }, undefined>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"drop-index">;
    id: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"backfill-completed">;
    relation: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
        rowKey: v.ObjectType<{
            columns: v.ArrayType<v.Type<string>>;
            type: v.Optional<"default" | "nothing" | "full" | "index">;
        }, undefined>;
    }, undefined>;
    columns: v.ArrayType<v.Type<string>>;
    watermark: v.Type<string>;
    status: v.Optional<{
        totalBytes?: number | undefined;
        rows: number;
        totalRows: number;
    }>;
}, undefined>]>]>]>, v.TupleType<[v.Type<"commit">, v.ObjectType<{
    tag: v.Type<"commit">;
}, undefined>, v.ObjectType<{
    watermark: v.Type<string>;
}, undefined>]>, v.TupleType<[v.Type<"rollback">, v.ObjectType<{
    tag: v.Type<"rollback">;
}, undefined>]>]>;
export declare const changeStreamMessageSchema: v.UnionType<[v.UnionType<[v.TupleType<[v.Type<"begin">, v.ObjectType<{
    tag: v.Type<"begin">;
    json: v.Optional<"p" | "s">;
    skipAck: v.Optional<boolean>;
}, undefined>, v.ObjectType<{
    commitWatermark: v.Type<string>;
}, undefined>]>, v.TupleType<[v.Type<"data">, v.UnionType<[v.UnionType<[v.ObjectType<{
    tag: v.Type<"insert">;
    relation: v.Type<{
        rowKey: {
            type?: "default" | "nothing" | "full" | "index" | undefined;
            columns: string[];
        };
        keyColumns?: string[] | undefined;
        replicaIdentity?: "default" | "nothing" | "full" | "index" | undefined;
        schema: string;
        name: string;
    }>;
    new: v.Type<Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"update">;
    relation: v.Type<{
        rowKey: {
            type?: "default" | "nothing" | "full" | "index" | undefined;
            columns: string[];
        };
        keyColumns?: string[] | undefined;
        replicaIdentity?: "default" | "nothing" | "full" | "index" | undefined;
        schema: string;
        name: string;
    }>;
    key: v.Type<Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue> | null>;
    new: v.Type<Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"delete">;
    relation: v.Type<{
        rowKey: {
            type?: "default" | "nothing" | "full" | "index" | undefined;
            columns: string[];
        };
        keyColumns?: string[] | undefined;
        replicaIdentity?: "default" | "nothing" | "full" | "index" | undefined;
        schema: string;
        name: string;
    }>;
    key: v.Type<Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"truncate">;
    relations: v.ArrayType<v.Type<{
        rowKey: {
            type?: "default" | "nothing" | "full" | "index" | undefined;
            columns: string[];
        };
        keyColumns?: string[] | undefined;
        replicaIdentity?: "default" | "nothing" | "full" | "index" | undefined;
        schema: string;
        name: string;
    }>>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"backfill">;
    relation: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
        rowKey: v.ObjectType<{
            columns: v.ArrayType<v.Type<string>>;
            type: v.Optional<"default" | "nothing" | "full" | "index">;
        }, undefined>;
    }, undefined>;
    columns: v.ArrayType<v.Type<string>>;
    watermark: v.Type<string>;
    rowValues: v.ArrayType<v.ArrayType<v.Type<import("../../../../../../shared/src/bigint-json.ts").JSONValue>>>;
    status: v.Optional<{
        totalBytes?: number | undefined;
        rows: number;
        totalRows: number;
    }>;
}, undefined>]>, v.UnionType<[v.ObjectType<{
    tag: v.Type<"create-table">;
    spec: v.ObjectType<Omit<{
        name: v.Type<string>;
        columns: v.Type<Record<string, {
            pgTypeClass?: "e" | "d" | "b" | "c" | "p" | "r" | "m" | undefined;
            elemPgTypeClass?: "e" | "d" | "b" | "c" | "p" | "r" | "m" | null | undefined;
            characterMaximumLength?: number | null | undefined;
            notNull?: boolean | null | undefined;
            dflt?: string | null | undefined;
            pos: number;
            dataType: string;
        }>>;
        primaryKey: v.Optional<string[]>;
    }, "schema"> & {
        schema: v.Type<string>;
    }, undefined>;
    metadata: v.Optional<{
        [x: string]: import("../../../../../../shared/src/bigint-json.ts").JSONValue;
        rowKey: Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>;
    }>;
    backfill: v.Optional<Record<string, Record<string, import("./json.ts").JSONValue | undefined>>>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"rename-table">;
    old: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
    new: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"update-table-metadata">;
    table: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
    old: v.ObjectType<{
        rowKey: v.Type<Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
    }, v.Type<import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
    new: v.ObjectType<{
        rowKey: v.Type<Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
    }, v.Type<import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"add-column">;
    table: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
    column: v.ObjectType<{
        name: v.Type<string>;
        spec: v.ObjectType<{
            pos: v.Type<number>;
            dataType: v.Type<string>;
            pgTypeClass: v.Optional<"e" | "d" | "b" | "c" | "p" | "r" | "m">;
            elemPgTypeClass: v.Optional<"e" | "d" | "b" | "c" | "p" | "r" | "m" | null>;
            characterMaximumLength: v.Optional<number | null>;
            notNull: v.Optional<boolean | null>;
            dflt: v.Optional<string | null>;
        }, undefined>;
    }, undefined>;
    tableMetadata: v.Optional<{
        [x: string]: import("../../../../../../shared/src/bigint-json.ts").JSONValue;
        rowKey: Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>;
    }>;
    backfill: v.Optional<Record<string, import("./json.ts").JSONValue | undefined>>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"update-column">;
    table: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
    old: v.ObjectType<{
        name: v.Type<string>;
        spec: v.ObjectType<{
            pos: v.Type<number>;
            dataType: v.Type<string>;
            pgTypeClass: v.Optional<"e" | "d" | "b" | "c" | "p" | "r" | "m">;
            elemPgTypeClass: v.Optional<"e" | "d" | "b" | "c" | "p" | "r" | "m" | null>;
            characterMaximumLength: v.Optional<number | null>;
            notNull: v.Optional<boolean | null>;
            dflt: v.Optional<string | null>;
        }, undefined>;
    }, undefined>;
    new: v.ObjectType<{
        name: v.Type<string>;
        spec: v.ObjectType<{
            pos: v.Type<number>;
            dataType: v.Type<string>;
            pgTypeClass: v.Optional<"e" | "d" | "b" | "c" | "p" | "r" | "m">;
            elemPgTypeClass: v.Optional<"e" | "d" | "b" | "c" | "p" | "r" | "m" | null>;
            characterMaximumLength: v.Optional<number | null>;
            notNull: v.Optional<boolean | null>;
            dflt: v.Optional<string | null>;
        }, undefined>;
    }, undefined>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"drop-column">;
    table: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
    column: v.Type<string>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"drop-table">;
    id: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"create-index">;
    spec: v.ObjectType<Omit<{
        name: v.Type<string>;
        tableName: v.Type<string>;
        unique: v.Type<boolean>;
        columns: v.Type<Record<string, "ASC" | "DESC">>;
    }, "schema"> & {
        schema: v.Type<string>;
    }, undefined>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"drop-index">;
    id: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
}, undefined>, v.ObjectType<{
    tag: v.Type<"backfill-completed">;
    relation: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
        rowKey: v.ObjectType<{
            columns: v.ArrayType<v.Type<string>>;
            type: v.Optional<"default" | "nothing" | "full" | "index">;
        }, undefined>;
    }, undefined>;
    columns: v.ArrayType<v.Type<string>>;
    watermark: v.Type<string>;
    status: v.Optional<{
        totalBytes?: number | undefined;
        rows: number;
        totalRows: number;
    }>;
}, undefined>]>]>]>, v.TupleType<[v.Type<"commit">, v.ObjectType<{
    tag: v.Type<"commit">;
}, undefined>, v.ObjectType<{
    watermark: v.Type<string>;
}, undefined>]>, v.TupleType<[v.Type<"rollback">, v.ObjectType<{
    tag: v.Type<"rollback">;
}, undefined>]>]>, v.TupleType<[v.Type<"control">, v.ObjectType<{
    tag: v.Type<"reset-required">;
    message: v.Optional<string>;
    errorDetails: v.Optional<Record<string, import("./json.ts").JSONValue | undefined>>;
}, undefined>]>, v.TupleType<[v.Type<"status">, v.ObjectType<{
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
}, undefined>]>]>;
// #endregion
