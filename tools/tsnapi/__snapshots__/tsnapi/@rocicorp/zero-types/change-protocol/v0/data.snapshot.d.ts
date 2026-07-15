// #region Types
export type BackfillCompleted = v.Infer<typeof backfillCompletedSchema>;
export type BackfillID = v.Infer<typeof backfillIDSchema>;
export type Change = MessageBegin | DataOrSchemaChange | MessageCommit | MessageRollback;
export type ChangeTag = Change['tag'];
export type ColumnAdd = v.Infer<typeof addColumnSchema>;
export type ColumnDrop = v.Infer<typeof dropColumnSchema>;
export type ColumnUpdate = v.Infer<typeof updateColumnSchema>;
export type DataChange = Satisfies<JSONObject,
v.Infer<typeof dataChangeSchema>>;
export type DataChangeTag = v.Infer<typeof dataChangeTagsSchema>;
export type DataOrSchemaChange = DataChange | SchemaChange;
export type DownloadStatus = v.Infer<typeof downloadStatusSchema>;
export type Identifier = v.Infer<typeof identifierSchema>;
export type IndexCreate = v.Infer<typeof createIndexSchema>;
export type IndexDrop = v.Infer<typeof dropIndexSchema>;
export type MessageBackfill = v.Infer<typeof backfillSchema>;
export type MessageBegin = v.Infer<typeof beginSchema>;
export type MessageCommit = v.Infer<typeof commitSchema>;
export type MessageDelete = v.Infer<typeof deleteSchema>;
export type MessageInsert = v.Infer<typeof insertSchema>;
export type MessageRelation = v.Infer<typeof relationSchema>;
export type MessageRollback = v.Infer<typeof rollbackSchema>;
export type MessageTruncate = v.Infer<typeof truncateSchema>;
export type MessageUpdate = v.Infer<typeof updateSchema>;
export type SchemaChange = Satisfies<JSONObject, v.Infer<typeof schemaChangeSchema>>;
export type SchemaChangeTag = v.Infer<typeof schemaChangeTagsSchema>;
export type TableCreate = v.Infer<typeof createTableSchema>;
export type TableDrop = v.Infer<typeof dropTableSchema>;
export type TableMetadata = v.Infer<typeof tableMetadataSchema>;
export type TableRename = v.Infer<typeof renameTableSchema>;
export type TableUpdateMetadata = v.Infer<typeof updateTableMetadataSchema>;
// #endregion

// #region Functions
export declare function isDataChange(_: Change): change is DataChange;
export declare function isSchemaChange(_: Change): change is SchemaChange;
// #endregion

// #region Variables
export declare const addColumnSchema: v.ObjectType<{
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
}, undefined>;
export declare const backfillCompletedSchema: v.ObjectType<{
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
}, undefined>;
export declare const backfillIDSchema: v.Type<Record<string, import("./json.ts").JSONValue | undefined>>;
export declare const backfillSchema: v.ObjectType<{
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
}, undefined>;
export declare const beginSchema: v.ObjectType<{
    tag: v.Type<"begin">;
    json: v.Optional<"p" | "s">;
    skipAck: v.Optional<boolean>;
}, undefined>;
export declare const commitSchema: v.ObjectType<{
    tag: v.Type<"commit">;
}, undefined>;
export declare const createIndexSchema: v.ObjectType<{
    tag: v.Type<"create-index">;
    spec: v.ObjectType<Omit<{
        name: v.Type<string>;
        tableName: v.Type<string>;
        unique: v.Type<boolean>;
        columns: v.Type<Record<string, "ASC" | "DESC">>;
    }, "schema"> & {
        schema: v.Type<string>;
    }, undefined>;
}, undefined>;
export declare const createTableSchema: v.ObjectType<{
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
}, undefined>;
export declare const dataChangeSchema: v.UnionType<[v.ObjectType<{
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
}, undefined>]>;
export declare const deleteSchema: v.ObjectType<{
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
}, undefined>;
export declare const downloadStatusSchema: v.ObjectType<{
    rows: v.Type<number>;
    totalRows: v.Type<number>;
    totalBytes: v.Optional<number>;
}, undefined>;
export declare const dropColumnSchema: v.ObjectType<{
    tag: v.Type<"drop-column">;
    table: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
    column: v.Type<string>;
}, undefined>;
export declare const dropIndexSchema: v.ObjectType<{
    tag: v.Type<"drop-index">;
    id: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
}, undefined>;
export declare const dropTableSchema: v.ObjectType<{
    tag: v.Type<"drop-table">;
    id: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
}, undefined>;
export declare const identifierSchema: v.ObjectType<{
    schema: v.Type<string>;
    name: v.Type<string>;
}, undefined>;
export declare const insertSchema: v.ObjectType<{
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
}, undefined>;
export declare const newRelationSchema: v.ObjectType<{
    schema: v.Type<string>;
    name: v.Type<string>;
    rowKey: v.ObjectType<{
        columns: v.ArrayType<v.Type<string>>;
        type: v.Optional<"default" | "nothing" | "full" | "index">;
    }, undefined>;
}, undefined>;
export declare const relationSchema: v.Type<{
    rowKey: {
        type?: "default" | "nothing" | "full" | "index" | undefined;
        columns: string[];
    };
    keyColumns?: string[] | undefined;
    replicaIdentity?: "default" | "nothing" | "full" | "index" | undefined;
    schema: string;
    name: string;
}>;
export declare const renameTableSchema: v.ObjectType<{
    tag: v.Type<"rename-table">;
    old: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
    new: v.ObjectType<{
        schema: v.Type<string>;
        name: v.Type<string>;
    }, undefined>;
}, undefined>;
export declare const rollbackSchema: v.ObjectType<{
    tag: v.Type<"rollback">;
}, undefined>;
export declare const rowSchema: v.Type<Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
export declare const schemaChangeSchema: v.UnionType<[v.ObjectType<{
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
}, undefined>]>;
export declare const tableMetadataSchema: v.ObjectType<{
    rowKey: v.Type<Record<string, import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
}, v.Type<import("../../../../../../shared/src/bigint-json.ts").JSONValue>>;
export declare const truncateSchema: v.ObjectType<{
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
}, undefined>;
export declare const updateColumnSchema: v.ObjectType<{
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
}, undefined>;
export declare const updateSchema: v.ObjectType<{
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
}, undefined>;
export declare const updateTableMetadataSchema: v.ObjectType<{
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
}, undefined>;
// #endregion
