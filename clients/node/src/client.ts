// The Trail client: a queue, a timer and a spool. `spec/client.md` is the contract.
//
// Platform-neutral: nothing here imports from `node:`. Node features (environment, hostname,
// process hooks, the folder spool) are reached through `globalThis.process` when it exists.

import { FolderSpool, KINDS, LocalStorageSpool, MemorySpool, countRecords, splitBody } from "./spool.ts";
import type { Spool, StorageLike } from "./spool.ts";
import { nowMs, uuid7 } from "./uuid.ts";

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export interface EmitContext { actor?: string; client?: string; traceId?: string; ts?: number }
export interface TrailProblem { kind: "rejected" | "mismatched" | "dropped" | "send"; at: string; message: string; fix: string }
export interface TrailOptions {
  source: string; url?: string; version?: string; host?: string; token?: string;
  spoolDir?: string;            // Node: a folder. Browser: ignored; localStorage is used.
  flushIntervalMs?: number; maxBatch?: number; maxQueue?: number; maxSpoolBytes?: number;
  strict?: boolean; onProblem?: (problem: TrailProblem) => void; fetch?: typeof fetch;
}
export interface TrailStats { sent: number; spooled: number; dropped: number; rejected: number; mismatched: number }

/** Rejected by `flush()` in strict mode, listing every problem since the last flush. */
export class TrailError extends Error {
  readonly problems: TrailProblem[];
  constructor(problems: TrailProblem[]) {
    const lines = problems.map((p) => `${p.kind} ${p.at}: ${p.message}${p.fix ? `\n  fix: ${p.fix}` : ""}`);
    super(`${problems.length} Trail problem(s):\n${lines.join("\n")}`);
    this.name = "TrailError";
    this.problems = problems;
  }
}

export const DEFAULT_URL = "http://127.0.0.1:8200";
const EXIT_TIMEOUT_MS = 2000;
const REQUEST_TIMEOUT_MS = 10_000;
const MAX_BACKOFF_MS = 60_000;
const KEEPALIVE_BUDGET = 60_000; // browsers allow 64 KB of keepalive bodies in flight
const EVENT = 0, METRIC = 1, ERROR = 2, LOG = 3;

interface Item { kind: number; text: string }

interface NodeProcess {
  env?: Record<string, string | undefined>;
  versions?: { node?: string };
  on?: (event: string, listener: (...args: unknown[]) => void) => unknown;
  off?: (event: string, listener: (...args: unknown[]) => void) => unknown;
  getBuiltinModule?: (id: string) => unknown;
}

interface Listening {
  addEventListener(type: string, listener: (event: never) => void): void;
  removeEventListener(type: string, listener: (event: never) => void): void;
}

interface BrowserGlobals {
  window?: Listening;
  document?: Listening & { visibilityState?: string };
  localStorage?: StorageLike;
}

function nodeProcess(): NodeProcess | undefined {
  const proc = (globalThis as unknown as { process?: NodeProcess }).process;
  return typeof proc?.versions?.node === "string" ? proc : undefined;
}

function browser(): BrowserGlobals | undefined {
  const g = globalThis as BrowserGlobals;
  return typeof g.window === "object" && typeof g.document === "object" ? g : undefined;
}

function env(name: string): string | undefined {
  const value = (globalThis as unknown as { process?: NodeProcess }).process?.env?.[name]?.trim();
  return value ? value : undefined;
}

function envFlag(name: string): boolean {
  return ["1", "true", "yes", "on"].includes((env(name) ?? "").toLowerCase());
}

function hostname(): string | undefined {
  try {
    const os = nodeProcess()?.getBuiltinModule?.("node:os") as { hostname(): string } | undefined;
    return os?.hostname() || undefined;
  } catch {
    return undefined;
  }
}

function describe(error: unknown): string {
  if (error instanceof Error) {
    const cause = (error as { cause?: unknown }).cause;
    return cause instanceof Error ? `${error.message} (${cause.message})` : error.message;
  }
  return String(error);
}

/** JSON that refuses what JSON cannot carry (NaN, Infinity, BigInt, cycles) instead of changing it. */
function toJson(value: unknown): string {
  return JSON.stringify(value, (key, item: unknown) => {
    if (typeof item === "number" && !Number.isFinite(item)) {
      throw new TypeError(`${key || "value"} is ${item}, which JSON cannot carry`);
    }
    return item;
  });
}

function isErrorLike(value: unknown): value is { name?: unknown; message: unknown; stack?: unknown } {
  return value instanceof Error ||
    (typeof value === "object" && value !== null && "message" in value && "name" in value);
}

function stringify(value: unknown): string {
  if (typeof value === "string") return value;
  try {
    const text = JSON.stringify(value);
    if (typeof text === "string") return text;
  } catch {
    // fall through
  }
  return String(value);
}

function sleep(ms: number): { promise: Promise<void>; cancel(): void } {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const promise = new Promise<void>((resolve) => {
    timer = setTimeout(resolve, ms);
    unref(timer);
  });
  return { promise, cancel: () => clearTimeout(timer) };
}

function unref(timer: unknown): void {
  (timer as { unref?: () => void } | undefined)?.unref?.();
}

/** What became of one post: finished with (taken, or dropped because a retry cannot succeed),
 * kept to retry, or refused with 413 and to be sent again as two halves. */
type Outcome = "done" | "retry" | "split";

const NOT_TRAIL_FIX = "check that TRAIL_URL points at Trail itself, and that nothing between the app and Trail " +
  "(a proxy, a sign-on page, a captive portal) answers in its place";

/** Queues records and sends them to Trail in the background. Never throws into the caller
 * unless `strict` is on, and then only from `flush()`. */
export class TrailClient {
  readonly source: string;
  readonly endpoint: string;
  readonly version: string | undefined;
  readonly host: string | undefined;
  readonly disabled: boolean;
  readonly strict: boolean;
  readonly flushIntervalMs: number;
  readonly maxBatch: number;
  readonly maxQueue: number;
  readonly maxSpoolBytes: number;
  readonly #token: string | undefined;
  readonly #onProblem: ((problem: TrailProblem) => void) | undefined;
  readonly #fetch: typeof fetch;

  #spool: Spool = new MemorySpool();
  #queue: Item[] = [];
  #timer: ReturnType<typeof setInterval> | undefined;
  #kickScheduled = false;
  #busy = false;
  #tail: Promise<unknown> = Promise.resolve();
  #closed = false;
  #backoffMs = 0;
  #nextAttempt = 0;
  #failing = false;
  #strictProblems: TrailProblem[] = [];
  #unreported = { queue: 0, spool: 0 };
  /** Spool entries being posted right now. The spool loop, the size bound and pagehide leave them alone. */
  #inFlight = new Set<string>();
  #spoolWriteFailing = false;
  #stats: TrailStats = { sent: 0, spooled: 0, dropped: 0, rejected: 0, mismatched: 0 };
  #exitFlushed = false;
  #processHooks = false;
  #browserHooks = false;
  #removers: (() => void)[] = [];

  constructor(options: TrailOptions) {
    this.source = options.source;
    this.disabled = envFlag("TRAIL_DISABLED");
    this.strict = options.strict ?? envFlag("TRAIL_STRICT");
    const base = options.url ?? env("TRAIL_URL") ?? DEFAULT_URL;
    this.endpoint = `${base.replace(/\/+$/, "")}/v1/ingest`;
    // An empty string passed in code means "leave it out".
    this.version = (options.version ?? env("TRAIL_VERSION")) || undefined;
    this.host = (options.host ?? env("TRAIL_HOST") ?? hostname()) || undefined;
    this.#token = (options.token ?? env("TRAIL_TOKEN")) || undefined;
    this.flushIntervalMs = Math.max(1, options.flushIntervalMs ?? 1000);
    this.maxBatch = Math.max(1, options.maxBatch ?? 200);
    this.maxQueue = Math.max(1, options.maxQueue ?? 10_000);
    this.maxSpoolBytes = Math.max(0, options.maxSpoolBytes ?? 50_000_000);
    this.#onProblem = options.onProblem;
    const injected = options.fetch;
    this.#fetch = (input, init) => (injected ?? globalThis.fetch)(input, init);
    if (this.disabled) return;

    const page = browser();
    const proc = nodeProcess();
    const folder = options.spoolDir ?? env("TRAIL_SPOOL_DIR");
    if (page?.localStorage) {
      this.#spool = new LocalStorageSpool(page.localStorage, this.source);
    } else if (proc && folder) {
      this.#spool = new FolderSpool(folder);
    }
    if (this.#spool.durable) {
      void this.#ready().then(() => {
        if (this.#safeNames().length > 0) this.#start(); // a previous run left batches
      });
    }
    if (page) this.#listenForPageHide(page);
    if (proc) this.#listenForExit(proc);
  }

  // ---------- recording ----------

  /** Called by generated code. Application code calls the generated functions instead. */
  recordEvent(name: string, props: Readonly<Record<string, JsonValue | undefined>>, context?: EmitContext): void {
    if (this.disabled) return;
    try {
      const record: Record<string, unknown> = { id: uuid7(), name, ts: context?.ts ?? nowMs() };
      if (context?.actor !== undefined) record.actor = context.actor;
      if (context?.client !== undefined) record.client = context.client;
      if (context?.traceId !== undefined) record.trace_id = context.traceId;
      record.props = props;
      this.#push(EVENT, record, `event ${name}`);
    } catch (error) {
      this.#internal(`event ${name}`, error);
    }
  }

  /** Called by generated code. Application code calls the generated functions instead. */
  recordMetric(
    name: string,
    value: number,
    labels: Readonly<Record<string, string>>,
    options: { unit?: string | undefined; kind: "gauge" | "counter"; ts?: number | undefined },
  ): void {
    if (this.disabled) return;
    try {
      if (typeof value !== "number" || !Number.isFinite(value)) {
        this.#dropOne(`metric ${name}`, `the value ${String(value)} is not a finite number`, "pass a number that is not NaN or Infinity");
        return;
      }
      const record: Record<string, unknown> = { name, value, ts: options.ts ?? nowMs(), labels };
      if (options.unit !== undefined) record.unit = options.unit;
      record.kind = options.kind;
      this.#push(METRIC, record, `metric ${name}`);
    } catch (error) {
      this.#internal(`metric ${name}`, error);
    }
  }

  /** Report anything thrown: an `Error` gives kind (its name), message and stack; anything else
   * is stringified, with kind `Error`. */
  error(
    error: unknown,
    options?: { severity?: "warning" | "error" | "fatal"; context?: Record<string, JsonValue>; traceId?: string },
  ): void {
    if (this.disabled) return;
    try {
      let kind = "Error";
      let message: string;
      let stack: string | undefined;
      if (isErrorLike(error)) {
        kind = typeof error.name === "string" && error.name ? error.name : "Error";
        message = typeof error.message === "string" ? error.message : stringify(error.message);
        stack = typeof error.stack === "string" && error.stack ? error.stack : undefined;
      } else {
        message = stringify(error);
      }
      const record: Record<string, unknown> = { id: uuid7(), ts: nowMs(), severity: options?.severity ?? "error", kind, message };
      if (stack !== undefined) record.stack = stack;
      if (options?.context !== undefined) record.context = options.context;
      if (options?.traceId !== undefined) record.trace_id = options.traceId;
      this.#push(ERROR, record, `error ${kind}`);
    } catch (failure) {
      this.#internal("error", failure);
    }
  }

  log(
    level: "trace" | "debug" | "info" | "warn" | "error" | "fatal",
    message: string,
    options?: { stream?: string; fields?: Record<string, JsonValue> },
  ): void {
    if (this.disabled) return;
    try {
      const record: Record<string, unknown> = { id: uuid7(), ts: nowMs(), level };
      if (options?.stream !== undefined) record.stream = options.stream;
      record.message = message;
      if (options?.fields !== undefined) record.fields = options.fields;
      this.#push(LOG, record, "log");
    } catch (error) {
      this.#internal("log", error);
    }
  }

  #push(kind: number, record: Record<string, unknown>, at: string): void {
    let text: string;
    try {
      text = toJson(record);
    } catch (error) {
      this.#dropOne(at, `cannot be sent as JSON: ${describe(error)}`,
        "pass only strings, finite numbers, booleans, null, and arrays and plain objects of those");
      return;
    }
    if (this.#closed) {
      this.#stats.dropped += 1;
      return;
    }
    this.#queue.push({ kind, text });
    this.#exitFlushed = false;
    if (this.#queue.length > this.maxQueue) {
      const over = this.#queue.length - this.maxQueue;
      this.#queue.splice(0, over);
      this.#stats.dropped += over;
      this.#unreported.queue += over;
    }
    this.#start();
    if (this.#queue.length >= this.maxBatch) this.#kick();
  }

  #dropOne(at: string, message: string, fix: string): void {
    this.#stats.dropped += 1;
    this.#problem("dropped", at, `${message}; the record was dropped`, fix);
  }

  #internal(at: string, error: unknown): void {
    try {
      this.#problem("dropped", at, `the client failed: ${describe(error)}; the record was dropped`, "report this as a trail-client bug");
    } catch {
      // nothing left to try
    }
  }

  // ---------- problems ----------

  #problem(kind: TrailProblem["kind"], at: string, message: string, fix: string): void {
    const problem: TrailProblem = { kind, at, message, fix };
    if (this.strict) this.#strictProblems.push(problem);
    try {
      this.#onProblem?.(problem);
    } catch {
      // a broken callback must not stop the sender
    }
  }

  #reportDrops(): void {
    const { queue, spool } = this.#unreported;
    this.#unreported = { queue: 0, spool: 0 };
    if (queue) {
      this.#problem("dropped", "queue", `${queue} record(s) dropped, oldest first: the queue holds maxQueue=${this.maxQueue}`,
        "check that Trail is reachable; set spoolDir so waiting batches go to disk, or raise maxQueue");
    }
    if (spool) {
      this.#problem("dropped", "spool", `${spool} record(s) dropped, oldest first: the spool holds maxSpoolBytes=${this.maxSpoolBytes}`,
        "check that Trail is reachable, or raise maxSpoolBytes");
    }
  }

  // ---------- scheduling ----------

  #start(): void {
    if (this.#timer !== undefined || this.#closed) return;
    this.#timer = setInterval(() => this.#tick(false), this.flushIntervalMs);
    unref(this.#timer); // never keeps a Node process alive
  }

  #kick(): void {
    if (this.#kickScheduled || this.#inBackoff()) return;
    this.#kickScheduled = true;
    const timer = setTimeout(() => {
      this.#kickScheduled = false;
      // Woken by a full batch, send only full batches; the rest waits for the interval.
      this.#tick(true);
    }, 0);
    unref(timer);
  }

  #tick(fullOnly: boolean): void {
    if (this.#busy || this.#closed) return;
    this.#busy = true;
    void this.#exclusive(() => this.#cycle(false, Date.now() + 2 * REQUEST_TIMEOUT_MS, fullOnly))
      .finally(() => { this.#busy = false; });
  }

  /** Runs `work` after every earlier piece of work, so only one cycle sends at a time. */
  #exclusive<T>(work: () => Promise<T>): Promise<T> {
    const run = this.#tail.then(work, work);
    this.#tail = run.catch(() => undefined);
    return run;
  }

  #inBackoff(): boolean {
    return Date.now() < this.#nextAttempt;
  }

  async #ready(): Promise<void> {
    try {
      await this.#spool.ready();
    } catch (error) {
      const where = this.#spool instanceof FolderSpool ? `\`${this.#spool.path}\`` : "the spool";
      this.#spool = new MemorySpool();
      this.#problem("send", "spoolDir", `cannot use ${where} as the spool (${describe(error)}); batches that fail wait in memory`,
        "pass a folder the process can create and write, like `/var/lib/shop/trail-spool`");
    }
  }

  #safeNames(): string[] {
    try {
      return this.#spool.names();
    } catch (error) {
      this.#problem("send", "spool", `cannot read the spool: ${describe(error)}`, "check the spool folder's permissions");
      return [];
    }
  }

  // ---------- sending ----------

  /** One pass: the spool oldest first, then the queue. True when nothing failed. */
  async #cycle(force: boolean, deadline: number, fullOnly: boolean): Promise<boolean> {
    try {
      await this.#ready();
      if (!force && this.#inBackoff()) {
        if (this.#spool.durable) this.#spoolQueue(); // keep waiting records on disk, not just in memory
        return false;
      }
      const stop = () => Date.now() >= deadline || (!force && this.#closed);
      return (await this.#drainSpool(deadline, stop)) && (await this.#drainQueue(deadline, fullOnly, stop));
    } catch (error) {
      this.#internal("sender", error);
      return false;
    } finally {
      this.#reportDrops();
    }
  }

  /** Posts the spool's entries oldest first. An entry split after a `413` is replaced by its two
   * halves, and the pass starts again from the oldest, so the halves go out in this pass too.
   * Each split leaves smaller entries, so the passes end. */
  async #drainSpool(deadline: number, stop: () => boolean): Promise<boolean> {
    for (;;) {
      let split = false;
      for (const name of this.#safeNames()) {
        if (stop()) return false;
        if (this.#inFlight.has(name)) continue; // posted by pagehide's keepalive right now
        this.#inFlight.add(name);
        let body: string | undefined;
        let outcome: Outcome;
        try {
          try {
            body = this.#spool.read(name);
          } catch (error) {
            this.#problem("send", `spool ${name}`, `cannot read: ${describe(error)}`, "check the spool folder's permissions");
            return false;
          }
          if (body === undefined) continue;
          outcome = await this.#deliver(body, countRecords(body), deadline);
        } finally {
          this.#inFlight.delete(name);
        }
        if (outcome === "retry") return false;
        if (outcome === "split") {
          if (!this.#split(body, name)) return false;
          split = true;
          break;
        }
        if (!this.#removeSent(name)) return false;
      }
      if (!split) return true;
    }
  }

  /** Delete a spool entry that got a final answer. False when it cannot be deleted (it is then
   * sent again later, which the ids make harmless). */
  #removeSent(name: string): boolean {
    try {
      this.#spool.remove(name);
      return true;
    } catch (error) {
      this.#problem("send", `spool ${name}`, `cannot delete a sent batch: ${describe(error)}`, "check the spool folder's permissions");
      return false;
    }
  }

  #take(fullOnly: boolean): Item[] {
    if (fullOnly && this.#queue.length < this.maxBatch) return [];
    return this.#queue.splice(0, this.maxBatch);
  }

  async #drainQueue(deadline: number, fullOnly: boolean, stop: () => boolean): Promise<boolean> {
    while (!stop()) {
      const items = this.#take(fullOnly);
      if (items.length === 0) return true;
      const outcome = await this.#sendBatch(items, deadline);
      if (outcome === "retry") return false;
      if (outcome === "split" && !(await this.#drainSpool(deadline, stop))) return false; // the halves are in the spool now
    }
    return this.#queue.length === 0;
  }

  /** Sends one batch from the queue: "done" when it is finished with, "split" when it was refused
   * with `413` and its two halves are now in the spool, "retry" when it is kept.
   *
   * With a durable spool the batch is written there before it is posted and removed only after a
   * final answer, so a process (or page) that dies mid-request leaves it for the next run. Without
   * one, or when the write fails, it is posted from memory and kept only if the post fails. */
  async #sendBatch(items: Item[], deadline: number): Promise<Outcome> {
    const body = this.#body(items);
    const name = this.#spool.durable ? this.#spoolBeforeSend(body) : undefined;
    if (name === undefined) {
      const outcome = await this.#deliver(body, items.length, deadline);
      if (outcome === "done" || (outcome === "split" && this.#split(body))) return outcome;
      if (this.#spool.durable) this.#requeue(items); // the spool could not be written a moment ago; already reported
      else this.#keep(items, body);
      return "retry";
    }
    let outcome: Outcome;
    try {
      outcome = await this.#deliver(body, items.length, deadline);
    } finally {
      this.#inFlight.delete(name);
    }
    if (outcome === "done") return this.#removeSent(name) ? "done" : "retry";
    if (outcome === "split") return this.#split(body, name) ? "split" : "retry";
    this.#stats.spooled += items.length;
    this.#boundSpool();
    return "retry";
  }

  /** Puts the two halves of a batch refused with `413` in the spool, in place of its own entry
   * `replacing` if it has one. The halves are written before that entry is removed, so a crash in
   * between leaves records twice (harmless: they have ids), never lost. False when they cannot be
   * written; the batch is then kept whole and tried again later. */
  #split(body: string, replacing?: string): boolean {
    const halves = splitBody(body);
    if (halves === undefined) return false;
    const written: string[] = [];
    try {
      for (const half of halves) written.push(this.#spool.write(half));
    } catch (error) {
      for (const name of written) {
        try {
          this.#spool.remove(name);
        } catch {
          // a leftover half is sent twice at worst, which the ids make harmless
        }
      }
      this.#problem("send", "spool", `cannot write the two halves of a batch Trail found too large (413): ${describe(error)}; the batch is kept whole`,
        "check the spool's permissions and free space");
      return false;
    }
    return replacing === undefined || this.#removeSent(replacing);
  }

  /** Writes a batch about to be posted to the spool and marks it in flight. Undefined when the
   * write fails; the batch is then sent from memory. */
  #spoolBeforeSend(body: string): string | undefined {
    let name: string;
    try {
      name = this.#writeEntry(body);
    } catch (error) {
      if (!this.#spoolWriteFailing) {
        this.#spoolWriteFailing = true;
        this.#problem("send", "spool", `cannot write to the spool: ${describe(error)}; batches are sent from memory until it works again`,
          "check the spool's permissions and free space");
      }
      return undefined;
    }
    this.#spoolWriteFailing = false;
    this.#inFlight.add(name);
    this.#boundSpool(); // makes room by dropping the oldest entries, never one in flight
    return name;
  }

  /** Writes one spool entry. A full localStorage makes room by dropping the oldest entry, as the
   * byte bound would; anything else is thrown. */
  #writeEntry(body: string): string {
    for (;;) {
      try {
        return this.#spool.write(body);
      } catch (error) {
        if ((error as { name?: string }).name === "QuotaExceededError" && this.#dropOldestEntry()) continue;
        throw error;
      }
    }
  }

  /** Puts a batch back at the front of the queue, dropping the oldest past `maxQueue`. */
  #requeue(items: Item[]): void {
    this.#queue.unshift(...items);
    if (this.#queue.length > this.maxQueue) {
      const over = this.#queue.length - this.maxQueue;
      this.#queue.splice(0, over);
      this.#stats.dropped += over;
      this.#unreported.queue += over;
    }
  }

  /** Writes everything queued to the spool, one entry per batch. Synchronous, for crash paths. */
  #spoolQueue(): void {
    for (;;) {
      const items = this.#take(false);
      if (items.length === 0) return;
      if (!this.#keep(items, this.#body(items))) return;
    }
  }

  /** Put a batch that was not sent into the spool; back in the queue if the spool fails. */
  #keep(items: Item[], body: string): boolean {
    try {
      this.#writeEntry(body);
    } catch (error) {
      this.#requeue(items);
      this.#problem("send", "spool", `cannot write to the spool: ${describe(error)}`, "check the spool's permissions and free space");
      return false;
    }
    this.#stats.spooled += items.length;
    this.#boundSpool();
    return true;
  }

  #dropOldestEntry(): boolean {
    const oldest = this.#spool.names().find((name) => !this.#inFlight.has(name));
    if (oldest === undefined) return false;
    const count = countRecords(this.#spool.read(oldest) ?? "");
    this.#spool.remove(oldest);
    this.#stats.dropped += count;
    this.#unreported.spool += count;
    return true;
  }

  /** Drops the oldest entries until the spool fits `maxSpoolBytes`. An entry in flight is never
   * dropped; once its post fails it is an ordinary entry and counts like any other. */
  #boundSpool(): void {
    try {
      let total = this.#spool.totalBytes();
      for (const name of this.#spool.names()) {
        if (total <= this.maxSpoolBytes) break;
        if (this.#inFlight.has(name)) continue;
        const body = this.#spool.read(name) ?? "";
        this.#spool.remove(name);
        total -= new TextEncoder().encode(body).length;
        const count = countRecords(body);
        this.#stats.dropped += count;
        this.#unreported.spool += count;
      }
    } catch (error) {
      this.#problem("send", "spool", `cannot trim the spool: ${describe(error)}`, "check the spool's permissions");
    }
  }

  /** The request body for a batch, in the order `spec/ingest.md` shows. */
  #body(items: Item[]): string {
    const parts = [`"source":${JSON.stringify(this.source)}`];
    if (this.version !== undefined) parts.push(`"version":${JSON.stringify(this.version)}`);
    if (this.host !== undefined) parts.push(`"host":${JSON.stringify(this.host)}`);
    const groups: string[][] = [[], [], [], []];
    for (const item of items) groups[item.kind]?.push(item.text);
    KINDS.forEach((name, index) => {
      const group = groups[index];
      if (group && group.length > 0) parts.push(`"${name}":[${group.join(",")}]`);
    });
    return `{${parts.join(",")}}`;
  }

  #headers(): Record<string, string> {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (this.#token) headers.Authorization = `Bearer ${this.#token}`;
    return headers;
  }

  async #post(body: string, deadline: number): Promise<{ status?: number; payload: string; failure: string }> {
    const timeout = Math.min(REQUEST_TIMEOUT_MS, Math.max(50, deadline - Date.now()));
    try {
      const response = await this.#fetch(this.endpoint, {
        method: "POST",
        headers: this.#headers(),
        body,
        redirect: "manual", // a redirect is kept and retried, never followed as a GET that loses the body
        signal: AbortSignal.timeout(timeout),
      });
      const payload = await response.text().catch(() => "");
      return { status: response.status, payload, failure: `HTTP ${response.status}` };
    } catch (error) {
      return { payload: "", failure: `no answer from ${this.endpoint}: ${describe(error)}` };
    }
  }

  /** "done" when the batch is finished with (sent, or dropped because a retry cannot succeed). */
  async #deliver(body: string, count: number, deadline: number): Promise<Outcome> {
    const { status, payload, failure } = await this.#post(body, deadline);
    return this.#outcome(status, payload, failure, count);
  }

  /** Only a 2xx whose body is Trail's ingest answer means Trail took the batch: a proxy's
   * catch-all page, a sign-on wall or a captive portal can answer 200 with HTML. A 413 for more
   * than one record is "split": the caller sends the batch again as two halves. */
  #outcome(status: number | undefined, payload: string, failure: string, count: number): Outcome {
    if (status !== undefined && status >= 200 && status < 300) {
      const answer = ingestAnswer(payload);
      if (answer === undefined) {
        this.#failed(`the ${status} answer from ${this.endpoint} did not come from Trail: its body is not Trail's ingest answer ` +
          `(a JSON object with \`accepted\`); ${preview(payload)}`, NOT_TRAIL_FIX);
        return "retry";
      }
      this.#succeeded();
      this.#readResponse(answer, count);
      return "done";
    }
    if (status === 413 && count > 1) {
      this.#succeeded();
      return "split";
    }
    if (status === 400 || status === 401 || status === 413) {
      this.#succeeded();
      this.#stats.dropped += count;
      this.#problem("dropped", this.endpoint, `${count} record(s) refused with ${status}: ${serverMessage(payload)}`, this.#dropFix(status));
      return "done";
    }
    this.#failed(failure);
    return "retry";
  }

  #dropFix(status: number): string {
    if (status === 401) return `pass token or set TRAIL_TOKEN to the write token for source \`${this.source}\``;
    if (status === 413) return "the record is over Trail's size limit on its own; send smaller props, messages or fields";
    return "fix what the server names; a retry of the same batch cannot succeed";
  }

  #readResponse(parsed: Record<string, unknown>, count: number): void {
    const rejected = entries(parsed, "rejected");
    const mismatched = entries(parsed, "mismatched");
    this.#stats.sent += Math.max(0, count - rejected.length);
    this.#stats.rejected += rejected.length;
    this.#stats.mismatched += mismatched.length;
    for (const entry of rejected) this.#problem("rejected", entry.at, entry.message, entry.fix);
    for (const entry of mismatched) this.#problem("mismatched", entry.at, entry.message, entry.fix);
  }

  #succeeded(): void {
    this.#backoffMs = 0;
    this.#nextAttempt = 0;
    this.#failing = false;
  }

  #failed(failure: string, fix = "check that Trail is running and TRAIL_URL points at it"): void {
    this.#backoffMs = this.#backoffMs === 0 ? 1000 : Math.min(MAX_BACKOFF_MS, this.#backoffMs * 2);
    this.#nextAttempt = Date.now() + this.#backoffMs;
    const first = !this.#failing;
    this.#failing = true;
    if (first || this.strict) {
      const where = this.#spool.durable ? "the spool" : "memory";
      this.#problem("send", this.endpoint, `${failure}; batches wait in ${where} and are retried with backoff`, fix);
    }
  }

  // ---------- flush, close, stats ----------

  /** Records waiting in the queue or the spool. `inFlight = false` leaves out spool entries being
   * posted right now (by pagehide's keepalive), which a flush cannot hurry. */
  #pending(inFlight = true): boolean {
    if (this.#queue.length > 0) return true;
    try {
      return this.#spool.names().some((name) => inFlight || !this.#inFlight.has(name));
    } catch {
      return true;
    }
  }

  /** Send what is queued and spooled now, ignoring backoff. Resolves true when both are empty.
   * In strict mode, rejects with `TrailError` listing every problem since the last flush. */
  async flush(timeoutMs = 5000): Promise<boolean> {
    if (this.disabled) return true;
    const deadline = Date.now() + Math.max(0, timeoutMs);
    const work = this.#exclusive(async () => {
      for (;;) {
        const ok = await this.#cycle(true, deadline, false);
        if (!this.#pending(false) || !ok || Date.now() >= deadline) return;
      }
    });
    const timer = sleep(Math.max(0, timeoutMs));
    await Promise.race([work.catch(() => undefined), timer.promise]);
    timer.cancel();
    await this.#ready();
    const empty = !this.#pending();
    if (this.strict) {
      const problems = this.#strictProblems;
      this.#strictProblems = [];
      if (!empty && !problems.some((p) => p.kind === "send")) {
        problems.push({ kind: "send", at: this.endpoint, message: "records are still waiting after flush", fix: "check that Trail is reachable, or pass a longer timeout" });
      }
      if (problems.length > 0) throw new TrailError(problems);
    }
    return empty;
  }

  /** Flush, stop the timer and listeners, and write what is left to a durable spool. Safe to call twice. */
  async close(timeoutMs = 5000): Promise<void> {
    if (this.#closed || this.disabled) return;
    this.#closed = true;
    if (this.#timer !== undefined) clearInterval(this.#timer);
    this.#timer = undefined;
    for (const remove of this.#removers) remove();
    this.#removers = [];
    try {
      await this.flush(timeoutMs);
    } catch {
      // strict problems are for flush(); close only finishes up
    }
    this.#persistQueue();
  }

  /** Synchronously moves the queue into a durable spool; what cannot be kept is counted as dropped. */
  #persistQueue(): void {
    try {
      if (this.#spool.durable) this.#spoolQueue();
    } catch (error) {
      this.#internal("spool", error);
    }
    const left = this.#queue.length;
    this.#queue = [];
    this.#stats.dropped += left;
    this.#reportDrops();
    if (left) {
      this.#problem("dropped", "close", `${left} record(s) were not sent before the client stopped`,
        "set spoolDir (Node) so they wait on disk for the next run");
    }
  }

  stats(): TrailStats {
    return { ...this.#stats };
  }

  // ---------- Node ----------

  #listenForExit(proc: NodeProcess): void {
    // The event loop is empty: give the queue one bounded flush. `beforeExit` fires again when
    // that finishes, so only flush again if something new was recorded.
    const beforeExit = () => {
      if (this.#exitFlushed || this.#closed || !this.#pending()) return;
      this.#exitFlushed = true;
      void this.flush(EXIT_TIMEOUT_MS).catch(() => false).then(() => {
        if (this.#spool.durable) this.#spoolQueue();
      });
    };
    // `process.exit()` or the end: whatever is still queued goes to the folder, synchronously.
    const exit = () => {
      if (!this.#closed && this.#queue.length > 0 && this.#spool.durable) this.#spoolQueue();
    };
    proc.on?.("beforeExit", beforeExit);
    proc.on?.("exit", exit);
    this.#removers.push(() => {
      proc.off?.("beforeExit", beforeExit);
      proc.off?.("exit", exit);
    });
  }

  /** Node: report `uncaughtException` and unhandled rejections, then leave the crash to Node.
   * Uses `uncaughtExceptionMonitor`, which sees both (an unhandled rejection reaches it with
   * origin `unhandledRejection`) without changing how Node handles them; an `unhandledRejection`
   * listener would stop Node from crashing. The queue is written to the spool synchronously. */
  installProcessHooks(): void {
    const proc = nodeProcess();
    if (this.disabled || this.#processHooks || !proc?.on) return;
    this.#processHooks = true;
    const monitor = (error: unknown, origin: unknown) => {
      try {
        this.error(error, { severity: "fatal", context: { origin: typeof origin === "string" ? origin : "uncaughtException" } });
        if (this.#spool.durable) this.#spoolQueue();
      } catch {
        // the process is going down; nothing else to try
      }
    };
    proc.on("uncaughtExceptionMonitor", monitor);
    this.#removers.push(() => proc.off?.("uncaughtExceptionMonitor", monitor));
  }

  /** Express error middleware: records the error and passes it on with `next(error)`. */
  expressErrorHandler(): (error: unknown, req: unknown, res: unknown, next: (error?: unknown) => void) => void {
    return (error, req, _res, next) => {
      const request = (req ?? {}) as { method?: unknown; originalUrl?: unknown; url?: unknown };
      const url = typeof request.originalUrl === "string" ? request.originalUrl : typeof request.url === "string" ? request.url : undefined;
      const context: Record<string, JsonValue> = {};
      if (typeof request.method === "string") context.method = request.method;
      if (url !== undefined) context.path = url.split("?")[0] ?? url; // the query may carry secrets
      this.error(error, { context });
      next(error);
    };
  }

  // ---------- browser ----------

  /** Browser: report `error` and `unhandledrejection` events. The browser still logs them. */
  installBrowserHooks(): void {
    const page = browser();
    if (this.disabled || this.#browserHooks || !page?.window) return;
    this.#browserHooks = true;
    const onError = (event: { error?: unknown; message?: unknown }) => this.error(event.error ?? event.message);
    const onRejection = (event: { reason?: unknown }) => this.error(event.reason);
    page.window.addEventListener("error", onError);
    page.window.addEventListener("unhandledrejection", onRejection);
    this.#removers.push(() => {
      page.window?.removeEventListener("error", onError);
      page.window?.removeEventListener("unhandledrejection", onRejection);
    });
  }

  #listenForPageHide(page: BrowserGlobals): void {
    const onHide = () => this.#sendOnHide();
    const onVisibility = () => {
      if (page.document?.visibilityState === "hidden") this.#sendOnHide();
    };
    page.window?.addEventListener("pagehide", onHide);
    page.document?.addEventListener("visibilitychange", onVisibility);
    this.#removers.push(() => {
      page.window?.removeEventListener("pagehide", onHide);
      page.document?.removeEventListener("visibilitychange", onVisibility);
    });
  }

  /** The page may be going away: send what is queued with `keepalive`, which outlives the page.
   * With localStorage, the batches are written to the spool first and removed when answered, so
   * a page that closes before the answer resends them on its next load (ids make that safe). */
  #sendOnHide(): void {
    if (this.#closed) return;
    try {
      const bodies: { body: string; count: number; name?: string }[] = [];
      if (this.#spool.durable) {
        this.#spoolQueue();
        for (const name of this.#spool.names()) {
          if (this.#inFlight.has(name)) continue; // already being posted; it stays in the spool if that fails
          const body = this.#spool.read(name);
          if (body !== undefined) bodies.push({ body, count: countRecords(body), name });
        }
      } else {
        for (let items = this.#take(false); items.length > 0; items = this.#take(false)) {
          bodies.push({ body: this.#body(items), count: items.length });
        }
      }
      let budget = KEEPALIVE_BUDGET;
      for (const entry of bodies) {
        const size = new TextEncoder().encode(entry.body).length;
        if (size > budget) break; // the rest waits in the spool for the next page load
        budget -= size;
        const name = entry.name;
        if (name !== undefined) this.#inFlight.add(name);
        void this.#fetch(this.endpoint, { method: "POST", headers: this.#headers(), body: entry.body, keepalive: true })
          .then(async (response) => {
            const outcome = this.#outcome(response.status, await response.text().catch(() => ""), `HTTP ${response.status}`, entry.count);
            if (outcome === "done" && name !== undefined) this.#spool.remove(name);
            if (outcome === "split") this.#split(entry.body, name); // the halves go at the next send
          })
          .catch(() => undefined)
          .finally(() => {
            if (name !== undefined) this.#inFlight.delete(name);
          });
      }
    } catch (error) {
      this.#internal("pagehide", error);
    }
  }
}

/** The body of a 2xx when it is Trail's answer to `POST /v1/ingest`: a JSON object with
 * `accepted` (`spec/ingest.md`). Undefined for anything else, like an HTML page. */
function ingestAnswer(payload: string): Record<string, unknown> | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(payload);
  } catch {
    return undefined;
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed) || !("accepted" in parsed)) return undefined;
  return parsed as Record<string, unknown>;
}

function preview(payload: string): string {
  const text = payload.slice(0, 80).split(/\s+/).filter(Boolean).join(" ");
  return text ? `it begins \`${text}\`` : "it is empty";
}

function entries(parsed: unknown, key: string): { at: string; message: string; fix: string }[] {
  if (typeof parsed !== "object" || parsed === null) return [];
  const items: unknown = (parsed as Record<string, unknown>)[key];
  if (!Array.isArray(items)) return [];
  const found: { at: string; message: string; fix: string }[] = [];
  for (const item of items as unknown[]) {
    if (typeof item !== "object" || item === null) continue;
    const entry = item as Record<string, unknown>;
    found.push({ at: String(entry.at ?? ""), message: String(entry.message ?? ""), fix: String(entry.fix ?? "") });
  }
  return found;
}

function serverMessage(payload: string): string {
  try {
    const parsed = JSON.parse(payload) as { error?: { at?: unknown; message?: unknown; fix?: unknown } };
    const problem = parsed.error;
    if (problem && typeof problem.message === "string") {
      const text = `${String(problem.at ?? "")}: ${problem.message}`;
      return typeof problem.fix === "string" && problem.fix ? `${text} (fix: ${problem.fix})` : text;
    }
  } catch {
    // not JSON
  }
  return payload.slice(0, 200) || "no body";
}
