// Where batches wait while Trail is unreachable. An entry is one request body, exactly as it
// would be posted, named `<uuid7>.json` so names sort by age (`spec/client.md`).
//
// Three spools, chosen when the client is made: a folder (Node), `localStorage` (browser), or
// memory (neither). Every method but `ready()` is synchronous, so a crashing process can still
// write its queue. Nothing here imports `node:` at the top: the folder spool loads `node:fs` with
// a dynamic import the first time it is used, so a browser bundle never touches it.

import { uuid7 } from "./uuid.ts";

export interface Spool {
  /** True when entries outlive the process (or page). */
  readonly durable: boolean;
  /** Resolves once the spool can be used. Rejects when it cannot (the client falls back to memory). */
  ready(): Promise<void>;
  /** Entry names, oldest first. */
  names(): string[];
  /** The entry's body, or undefined when it has gone. */
  read(name: string): string | undefined;
  /** Store a body as a new entry and return its name. */
  write(body: string): string;
  remove(name: string): void;
  totalBytes(): number;
}

export const KINDS = ["events", "metrics", "errors", "logs"] as const;

const encoder = new TextEncoder();

export function byteLength(text: string): number {
  return encoder.encode(text).length;
}

/** How many records a request body holds; 0 when it cannot be read. */
export function countRecords(body: string): number {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return 0;
  }
  if (typeof parsed !== "object" || parsed === null) return 0;
  let total = 0;
  for (const kind of KINDS) {
    const items: unknown = (parsed as Record<string, unknown>)[kind];
    if (Array.isArray(items)) total += items.length;
  }
  return total;
}

/** A request body of two or more records cut into two halves, each with the batch fields
 * (`source`, `version`, `host`) of the whole; undefined when it cannot be read or holds fewer
 * than two records. For a `413`: sent on their own, one oversized record ends up alone. */
export function splitBody(body: string): [string, string] | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return undefined;
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return undefined;
  const data = parsed as Record<string, unknown>;
  const records: [string, unknown][] = [];
  for (const kind of KINDS) {
    const items: unknown = data[kind];
    if (Array.isArray(items)) for (const item of items as unknown[]) records.push([kind, item]);
  }
  if (records.length < 2) return undefined;
  const fields = Object.fromEntries(Object.entries(data).filter(([key]) => !(KINDS as readonly string[]).includes(key)));
  const middle = Math.floor(records.length / 2);
  return [assemble(fields, records.slice(0, middle)), assemble(fields, records.slice(middle))];
}

function assemble(fields: Record<string, unknown>, records: [string, unknown][]): string {
  const body: Record<string, unknown> = { ...fields };
  for (const kind of KINDS) {
    const items = records.filter(([of]) => of === kind).map(([, record]) => record);
    if (items.length > 0) body[kind] = items;
  }
  return JSON.stringify(body);
}

function entryName(): string {
  return `${uuid7()}.json`;
}

export class MemorySpool implements Spool {
  readonly durable = false;
  #entries = new Map<string, string>();

  ready(): Promise<void> {
    return Promise.resolve();
  }
  names(): string[] {
    return [...this.#entries.keys()].sort();
  }
  read(name: string): string | undefined {
    return this.#entries.get(name);
  }
  write(body: string): string {
    const name = entryName();
    this.#entries.set(name, body);
    return name;
  }
  remove(name: string): void {
    this.#entries.delete(name);
  }
  totalBytes(): number {
    let total = 0;
    for (const body of this.#entries.values()) total += byteLength(body);
    return total;
  }
}

/** The part of the Web Storage API the spool uses, so tests can pass a stand-in. */
export interface StorageLike {
  readonly length: number;
  key(index: number): string | null;
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

/** Entries are keys `trail-spool:<source>:<uuid7>.json` in `localStorage`. */
export class LocalStorageSpool implements Spool {
  readonly durable = true;
  readonly #storage: StorageLike;
  readonly #prefix: string;

  constructor(storage: StorageLike, source: string) {
    this.#storage = storage;
    this.#prefix = `trail-spool:${source}:`;
  }

  ready(): Promise<void> {
    return Promise.resolve();
  }
  names(): string[] {
    const found: string[] = [];
    for (let i = 0; i < this.#storage.length; i++) {
      const key = this.#storage.key(i);
      if (key !== null && key.startsWith(this.#prefix)) found.push(key.slice(this.#prefix.length));
    }
    return found.sort();
  }
  read(name: string): string | undefined {
    return this.#storage.getItem(this.#prefix + name) ?? undefined;
  }
  write(body: string): string {
    const name = entryName();
    this.#storage.setItem(this.#prefix + name, body); // may throw QuotaExceededError; the client handles it
    return name;
  }
  remove(name: string): void {
    this.#storage.removeItem(this.#prefix + name);
  }
  totalBytes(): number {
    let total = 0;
    for (const name of this.names()) total += byteLength(this.read(name) ?? "");
    return total;
  }
}

type Fs = typeof import("node:fs");

interface NodeProcessLike {
  getBuiltinModule?: (id: string) => unknown;
}

/** One file per batch in a folder. The folder may hold files from earlier runs of the service. */
export class FolderSpool implements Spool {
  readonly durable = true;
  readonly path: string;
  #fs: Fs | undefined;
  #loading: Promise<void> | undefined;

  constructor(path: string) {
    this.path = path.replace(/[\\/]+$/, "") || path;
  }

  ready(): Promise<void> {
    this.#loading ??= import("node:fs").then((fs) => {
      fs.mkdirSync(this.path, { recursive: true });
      this.#fs = fs;
    });
    return this.#loading;
  }

  /** The loaded module. A crash before the dynamic import has finished still gets one, from
   * `process.getBuiltinModule`, which is synchronous. */
  #sync(): Fs {
    if (this.#fs === undefined) {
      const proc = (globalThis as unknown as { process?: NodeProcessLike }).process;
      const fs = proc?.getBuiltinModule?.("node:fs") as Fs | undefined;
      if (fs === undefined) throw new Error("node:fs is not loaded yet");
      fs.mkdirSync(this.path, { recursive: true });
      this.#fs = fs;
    }
    return this.#fs;
  }

  #file(name: string): string {
    return `${this.path}/${name}`;
  }

  names(): string[] {
    return this.#sync()
      .readdirSync(this.path)
      .filter((name) => name.endsWith(".json") && !name.startsWith("."))
      .sort();
  }
  read(name: string): string | undefined {
    try {
      return this.#sync().readFileSync(this.#file(name), "utf8");
    } catch (error) {
      if ((error as { code?: string }).code === "ENOENT") return undefined;
      throw error;
    }
  }
  write(body: string): string {
    const fs = this.#sync();
    const name = entryName();
    // Written under a hidden name and renamed, so a reader never sees half a file.
    const temporary = this.#file(`.${name}.tmp`);
    fs.writeFileSync(temporary, body);
    fs.renameSync(temporary, this.#file(name));
    return name;
  }
  remove(name: string): void {
    this.#sync().rmSync(this.#file(name), { force: true });
  }
  totalBytes(): number {
    const fs = this.#sync();
    let total = 0;
    for (const name of this.names()) {
      try {
        total += fs.statSync(this.#file(name)).size;
      } catch {
        // sent by another process meanwhile
      }
    }
    return total;
  }
}
