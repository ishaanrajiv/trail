// A fake Trail for the tests, plus helpers for temporary folders, child processes and the
// TRAIL_* environment.

import { spawn } from "node:child_process";
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { createServer } from "node:http";
import type { Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { TrailClient } from "../src/index.ts";
import type { TrailOptions, TrailProblem } from "../src/index.ts";

export const INDEX = fileURLToPath(new URL("../src/index.ts", import.meta.url));

export type Body = Record<string, unknown> & {
  source?: string;
  events?: Record<string, unknown>[];
  metrics?: Record<string, unknown>[];
  errors?: Record<string, unknown>[];
  logs?: Record<string, unknown>[];
};

export interface Received { path: string; headers: Record<string, string | string[] | undefined>; raw: string; status: number }

export function parse(raw: string): Body {
  return JSON.parse(raw) as Body;
}

/** `POST /v1/ingest` on a loopback port. `answer()` chooses the reply; `down()` closes the port
 * (no answer) and `up()` opens the same port again. `hold()` keeps every request waiting for its
 * answer until `release()`; `arrived` counts requests read, answered or not. `html` makes a 200
 * answer a web page instead of Trail's answer, as a proxy's catch-all or a sign-on page would.
 * `limit` answers 413 to a request with more records than it, and `maxBytes` to a larger body. */
export class FakeTrail {
  requests: Received[] = [];
  status = 200;
  rejected: Record<string, string>[] = [];
  mismatched: Record<string, string>[] = [];
  delayMs = 0;
  html = false;
  limit: number | undefined;
  maxBytes: number | undefined;
  port = 0;
  arrived = 0;
  #held: (() => void)[] | undefined;
  #server: Server | undefined;

  get url(): string {
    return `http://127.0.0.1:${this.port}`;
  }

  hold(): void {
    this.#held ??= [];
  }

  release(): void {
    const held = this.#held ?? [];
    this.#held = undefined;
    for (const answer of held) answer();
  }

  answer(status: number, extra: { rejected?: Record<string, string>[]; mismatched?: Record<string, string>[] } = {}): void {
    this.status = status;
    this.rejected = extra.rejected ?? [];
    this.mismatched = extra.mismatched ?? [];
  }

  async up(): Promise<void> {
    const server = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        const raw = Buffer.concat(chunks).toString("utf8");
        this.arrived += 1;
        const reply = () => {
          let status = this.status;
          if ((this.limit !== undefined && records(raw) > this.limit) || (this.maxBytes !== undefined && Buffer.byteLength(raw) > this.maxBytes)) {
            status = 413;
          }
          this.requests.push({ path: req.url ?? "", headers: req.headers, raw, status });
          let body: unknown;
          if (status === 200) {
            let parsed: Body = {};
            try {
              parsed = parse(raw);
            } catch {
              // counted as nothing
            }
            const accepted = Object.fromEntries(["events", "metrics", "errors", "logs"].map((k) => [k, (parsed[k] as unknown[] | undefined)?.length ?? 0]));
            body = { accepted, duplicates: 0, rejected: this.rejected, mismatched: this.mismatched };
          } else if (status === 400) {
            body = { error: { at: "source", message: "`Shop` is not a source name", fix: "use lowercase letters, like `shop`" } };
          } else {
            body = { error: { at: "", message: `status ${status}`, fix: "" } };
          }
          if (status === 200 && this.html) {
            res.writeHead(status, { "Content-Type": "text/html" });
            res.end("<!doctype html><html><body>Sign in to continue</body></html>");
            return;
          }
          res.writeHead(status, { "Content-Type": "application/json" });
          res.end(JSON.stringify(body));
        };
        const answer = () => {
          if (this.delayMs) setTimeout(reply, this.delayMs);
          else reply();
        };
        if (this.#held) this.#held.push(answer);
        else answer();
      });
    });
    await new Promise<void>((resolve) => server.listen(this.port, "127.0.0.1", resolve));
    const address = server.address();
    if (address && typeof address === "object") this.port = address.port;
    this.#server = server;
  }

  async down(): Promise<void> {
    const server = this.#server;
    this.#server = undefined;
    if (!server) return;
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }

  bodies(): Body[] {
    return this.requests.map((r) => parse(r.raw));
  }

  async waitFor(count: number, timeoutMs = 3000): Promise<boolean> {
    return waitUntil(() => this.requests.length >= count, timeoutMs);
  }
}

function records(raw: string): number {
  try {
    const body = parse(raw);
    return (body.events?.length ?? 0) + (body.metrics?.length ?? 0) + (body.errors?.length ?? 0) + (body.logs?.length ?? 0);
  } catch {
    return 0;
  }
}

export async function waitUntil(condition: () => boolean, timeoutMs = 3000): Promise<boolean> {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    if (condition()) return true;
    await sleep(10);
  }
  return condition();
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** One test's world: a fake Trail, a spool folder, a clean environment, and clients closed at the end. */
export class World {
  fake = new FakeTrail();
  folder = mkdtempSync(join(tmpdir(), "trail-client-"));
  spool = join(this.folder, "spool");
  problems: TrailProblem[] = [];
  #clients: TrailClient[] = [];
  #savedEnv: Record<string, string | undefined> = {};

  static async create(): Promise<World> {
    const world = new World();
    for (const key of Object.keys(process.env)) {
      if (key.startsWith("TRAIL_")) {
        world.#savedEnv[key] = process.env[key];
        delete process.env[key];
      }
    }
    await world.fake.up();
    return world;
  }

  client(options: Partial<TrailOptions> = {}): TrailClient {
    const client = new TrailClient({
      source: "shop",
      url: this.fake.url,
      host: "web-1",
      spoolDir: this.spool,
      flushIntervalMs: 50,
      onProblem: (problem) => this.problems.push(problem),
      ...options,
    });
    this.#clients.push(client);
    return client;
  }

  kinds(): string[] {
    return this.problems.map((p) => p.kind);
  }

  spoolFiles(folder = this.spool): string[] {
    try {
      return readdirSync(folder).filter((n) => n.endsWith(".json") && !n.startsWith(".")).sort();
    } catch {
      return [];
    }
  }

  readSpool(name: string): string {
    return readFileSync(join(this.spool, name), "utf8");
  }

  async dispose(): Promise<void> {
    for (const client of this.#clients) await client.close(300);
    await this.fake.down();
    for (const key of Object.keys(process.env)) if (key.startsWith("TRAIL_")) delete process.env[key];
    Object.assign(process.env, this.#savedEnv);
    rmSync(this.folder, { recursive: true, force: true });
  }
}

/** Runs ES module code in a child Node, with TRAIL_* taken from `env` only. */
export function runChild(code: string, env: Record<string, string>): Promise<{ code: number | null; stdout: string; stderr: string }> {
  const clean = Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith("TRAIL_") && !k.startsWith("NODE_TEST")));
  return new Promise((resolve) => {
    const child = spawn(process.execPath, ["--input-type=module", "-e", code], { env: { ...clean, ...env }, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d: Buffer) => (stdout += d.toString()));
    child.stderr.on("data", (d: Buffer) => (stderr += d.toString()));
    const timer = setTimeout(() => child.kill("SIGKILL"), 20_000);
    child.on("close", (exitCode) => {
      clearTimeout(timer);
      resolve({ code: exitCode, stdout, stderr });
    });
  });
}

/** A Web Storage stand-in with an optional quota in characters, like a browser's. */
export class MemoryStorage {
  #items = new Map<string, string>();
  quota: number;

  constructor(quota = Infinity) {
    this.quota = quota;
  }
  get length(): number {
    return this.#items.size;
  }
  key(index: number): string | null {
    return [...this.#items.keys()][index] ?? null;
  }
  getItem(key: string): string | null {
    return this.#items.get(key) ?? null;
  }
  setItem(key: string, value: string): void {
    let used = key.length + value.length;
    for (const [k, v] of this.#items) if (k !== key) used += k.length + v.length;
    if (used > this.quota) {
      const error = new Error("the quota has been exceeded");
      error.name = "QuotaExceededError";
      throw error;
    }
    this.#items.set(key, value);
  }
  removeItem(key: string): void {
    this.#items.delete(key);
  }
  keys(): string[] {
    return [...this.#items.keys()];
  }
}
