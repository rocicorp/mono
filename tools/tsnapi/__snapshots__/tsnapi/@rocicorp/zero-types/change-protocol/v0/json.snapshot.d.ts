// #region Types
export type JSONObject = {
    [key: string]: JSONValue | undefined;
};
export type JSONValue = null | string | boolean | number | Array<JSONValue> | JSONObject;
// #endregion

// #region Variables
export declare const jsonObjectSchema: v.Type<Record<string, JSONValue | undefined>>;
export declare const jsonValueSchema: v.Type<JSONValue>;
// #endregion
