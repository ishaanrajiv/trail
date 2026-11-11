// trail-client: send events, metrics, errors and logs to Trail, from Node 22.18+ or a browser.
// Application code calls the typed functions `trail gen` writes; those call `recordEvent` and
// `recordMetric`. The contract is `spec/client.md` in the Trail repository.

export { TrailClient, TrailError, DEFAULT_URL } from "./client.ts";
export type { EmitContext, JsonValue, TrailOptions, TrailProblem, TrailStats } from "./client.ts";
export { uuid7 } from "./uuid.ts";
