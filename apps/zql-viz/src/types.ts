/* oxlint-disable @typescript-eslint/no-explicit-any */
export interface QueryHistoryItem {
  id: string;
  query: string; // The query string passed to run() or full code if no run() call
  fullCode?: string; // The complete code snippet (for re-execution)
  timestamp: Date;
  result?: any;
  error?: string;
}

export interface Schema {
  [key: string]: any;
}

export interface Node {
  id: number;
  name: string;
  type: string;
}

export interface Edge {
  source: number;
  dest: number;
}

export interface Graph {
  nodes: Node[];
  edges: Edge[];
}

export type Result = {
  ast: unknown | undefined;
  graph: Graph | undefined;
  remoteRunResult: RemoteRunResult | undefined;
};

export type RemoteRunResult = {
  warnings: string[];
  syncedRows: Record<string, Record<string, unknown>[]>;
  syncedRowCount: number;
  start: number;
  end: number;
  afterPermissions: string | undefined;
  // record of { [tableName: string]: { [queryName: string]: number } }
  vendedRowCounts: Record<string, Record<string, number>> | undefined;
  vendedRows: Record<string, Record<string, number>> | undefined;
  sqlitePlans: Record<string, string[]> | undefined;
};
