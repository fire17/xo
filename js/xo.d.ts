export type Path = readonly string[];

export interface XOProtocolErrorDetail {
  readonly [key: string]: unknown;
}

export declare class XOProtocolError extends Error {
  readonly code: string;
  readonly detail: XOProtocolErrorDetail;
}

export type ConnectionState =
  | "connecting"
  | "handshaking"
  | "catching_up"
  | "ready"
  | "backoff"
  | "disconnected"
  | "closed";

export interface StateChange {
  readonly state: ConnectionState;
  readonly error?: unknown;
  readonly revision: number;
}

export interface XOChange {
  readonly kind: "snapshot" | "event" | "tx" | "derived" | "error";
  readonly revision?: number;
  readonly path?: Path;
  readonly value?: unknown;
  readonly events?: readonly unknown[];
  readonly error?: unknown;
}

export interface Subscription {
  readonly close: () => void;
  readonly cancel: () => void;
}

export interface CreateXOOptions {
  readonly url: `ws://${string}`;
  readonly namespace: string;
  readonly token: string;
  readonly prefixes?: readonly Path[];
  readonly writable?: boolean;
  readonly socketFactory?: (url: string) => WebSocket;
  readonly reconnect?: boolean;
  readonly minBackoff?: number;
  readonly maxBackoff?: number;
  readonly maxPending?: number;
  readonly maxSeen?: number;
  readonly onState?: (change: StateChange) => void;
}

export interface XONode {
  readonly path: Path;
  readonly exists: boolean;
  readonly hasValue: boolean;
  readonly value: unknown;
  readonly derived: XONode;
  set(value: unknown): Promise<unknown>;
  delete(): Promise<unknown>;
  at(path: string | Path): XONode;
  subscribe(callback: (change: XOChange) => void): Subscription;
  toJSON(): unknown;
  (): unknown;
  (value: unknown): Promise<unknown>;
  readonly [key: string]: unknown;
}

export declare function createXO(options: CreateXOOptions): XONode;
export declare function closeXO(node: XONode): void;
