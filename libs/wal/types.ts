export interface WalLike {
  open(): Promise<void>;
  append(obj: any): Promise<void>;
  flush(): Promise<void>;
  close(): Promise<void>;
  // async generator yielding entries (decoded)
  scan(minOffset?: number): AsyncGenerator<any, void, unknown>;
  // optional richer scans
  scanWithOffsets?(minOffset?: number): AsyncGenerator<{ value: any; start: number; end: number }, void, unknown>;
  reverseScan?(): AsyncGenerator<any, void, unknown>;
  // metrics bag
  metrics: any;
  // optional helper returning current durable end offset
  currentEndOffset?: () => number;
}
