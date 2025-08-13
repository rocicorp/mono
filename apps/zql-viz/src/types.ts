/* eslint-disable @typescript-eslint/no-explicit-any */
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
