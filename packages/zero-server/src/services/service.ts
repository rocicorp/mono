export interface Service {
  readonly id: string;

  start(): Promise<void>;
  stop(): Promise<void>;
}
