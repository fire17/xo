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

export type XOOperation =
  | { readonly kind: "set"; readonly path: string | Path; readonly value: unknown }
  | { readonly kind: "clear"; readonly path: string | Path }
  | { readonly kind: "delete"; readonly path: string | Path }
  | { readonly kind: "restore"; readonly path: string | Path; readonly node: XONodeImage };

export interface XONodeImage {
  readonly $value?: unknown;
  readonly $children: readonly (readonly [string, XONodeImage])[];
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
  readonly revision: number;
  readonly exists: boolean;
  readonly hasValue: boolean;
  readonly value: unknown;
  readonly derived: XONode;
  readonly keys: readonly string[];
  readonly values: readonly unknown[];
  readonly entries: readonly (readonly [string, unknown])[];
  get(defaultValue?: unknown): unknown;
  set(value: unknown): Promise<unknown>;
  clear(): Promise<unknown>;
  delete(): Promise<unknown>;
  restore(image: XONodeImage): Promise<unknown>;
  transaction(operations: readonly XOOperation[]): Promise<unknown>;
  at(path: string | Path): XONode;
  has(path: string | Path): boolean;
  subscribe(callback: (change: XOChange) => void): Subscription;
  toJSON(): unknown;
  [Symbol.iterator](): Iterator<string>;
  (): unknown;
  (value: unknown): Promise<unknown>;
  readonly [key: string]: unknown;
}

export declare function createXO(options: CreateXOOptions): XONode;
export declare function closeXO(node: XONode): void;
