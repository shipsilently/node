export type FlagValue = boolean | string | number | Record<string, unknown>;

export interface UserContext {
  userId?: string;
  email?: string;
  country?: string;
  plan?: string;
  [key: string]: string | number | boolean | undefined;
}

export interface EvaluationResult {
  flagKey: string;
  value: FlagValue;
  reason: 'default' | 'rule_match' | 'rollout' | 'flag_disabled' | 'flag_not_found';
  ruleId?: string;
}

export interface AnalyticsConfig {
  /** Buffer + submit evaluation analytics. Defaults to true. */
  enabled?: boolean;
  /** Flush cadence in milliseconds. Defaults to 30_000ms. Min 1000ms. */
  flushIntervalMs?: number;
  /** Force a flush once buffered events reach this size. Defaults to 100. */
  maxBatchSize?: number;
  /** Drop events when the buffer exceeds this size. Defaults to 5000. */
  maxBufferSize?: number;
  /**
   * Sampling rate in [0, 1]. 1.0 = record every evaluation, 0.5 = record half.
   * Defaults to 1.0. Useful for very-high-throughput workloads where the
   * server-side aggregation already provides enough fidelity.
   */
  samplingRate?: number;
}

export interface RetryConfig {
  /** Backoff base for the first reconnect attempt. Defaults to 1_000ms. */
  baseDelayMs?: number;
  /** Backoff ceiling. Defaults to 60_000ms. */
  maxDelayMs?: number;
  /**
   * Treat the stream as dead after this much silence. The server heartbeats
   * every 30s, so the default (90_000ms) tolerates two missed beats before
   * reconnecting.
   */
  heartbeatTimeoutMs?: number;
  /**
   * Set to false to restore the legacy behavior: any stream failure downgrades
   * to polling for the rest of the subscription, with no SSE reconnects.
   * Defaults to true.
   */
  enabled?: boolean;
}

export interface ShipSilentlyConfig {
  apiKey: string;
  /** Defaults to the production ShipSilently API */
  apiUrl?: string;
  /** Override the fetch implementation — useful for testing */
  fetch?: typeof globalThis.fetch;
  /** Analytics submission settings — see AnalyticsConfig. */
  analytics?: AnalyticsConfig;
  /** Reconnect/backoff behavior — see RetryConfig. */
  retry?: RetryConfig;
}

/**
 * Connection state reported through `stream()`'s `onStateChange`:
 * - `streaming`: live SSE connection established.
 * - `reconnecting`: stream lost; polling keeps data fresh while SSE reconnect
 *   attempts continue with backoff.
 * - `polling`: polling is the terminal mode for this subscription (free plan,
 *   no EventSource in the runtime, or `retry.enabled: false` after a failure).
 */
export type StreamConnectionState = 'streaming' | 'polling' | 'reconnecting';

export type FlagsUpdateCallback = (flags: Record<string, EvaluationResult>) => void;

const DEFAULT_API_URL = 'https://shipsilently-cf.workers.dev';

interface PendingAnalyticsEvent {
  flagKey: string;
  variationKey: string;
  reason: string;
  latencyMs: number;
  timestamp: number;
}

const DEFAULT_FLUSH_INTERVAL_MS = 30_000;
const DEFAULT_MAX_BATCH_SIZE = 100;
const DEFAULT_MAX_BUFFER_SIZE = 5_000;
const MIN_FLUSH_INTERVAL_MS = 1_000;
const DEFAULT_RETRY_BASE_DELAY_MS = 1_000;
const DEFAULT_RETRY_MAX_DELAY_MS = 60_000;
const DEFAULT_HEARTBEAT_TIMEOUT_MS = 90_000;
/**
 * How long a stream connection must stay up before the backoff ladder resets.
 * Resetting on `connected` alone would let a server that accepts and
 * immediately drops connections get hammered at the base delay forever.
 */
const HEALTHY_CONNECTION_RESET_MS = 30_000;
/** A retrying SDK must not turn one incident into thousands of log lines. */
const WARN_DEDUP_INTERVAL_MS = 60_000;
/**
 * The ingestion endpoint rejects batches larger than this with a 400 (see
 * `ingestSchema` in the worker's analytics route). A single flush may need to
 * drain more than this — e.g. after re-buffering during an outage — so the
 * flush sends sequential chunks of at most this size instead of one oversized
 * request that would be rejected outright and silently lose every event.
 */
const MAX_EVENTS_PER_REQUEST = 1_000;

function variationKeyFromValue(value: FlagValue): string {
  let raw: string;
  try {
    raw = JSON.stringify(value);
  } catch {
    raw = String(value);
  }
  if (raw.length > 200) raw = `${raw.slice(0, 197)}…`;
  return raw;
}

export class ShipSilentlyClient {
  private readonly apiKey: string;
  private readonly apiUrl: string;
  private readonly fetch: typeof globalThis.fetch;
  private cache: Record<string, EvaluationResult> = {};
  private analyticsConfig: Required<AnalyticsConfig>;
  private analyticsBuffer: PendingAnalyticsEvent[] = [];
  private analyticsTimer: ReturnType<typeof setInterval> | null = null;
  private retryConfig: Required<RetryConfig>;
  private lastWarnAt = new Map<string, number>();

  constructor(config: ShipSilentlyConfig) {
    this.apiKey = config.apiKey;
    this.apiUrl = (config.apiUrl ?? DEFAULT_API_URL).replace(/\/$/, '');
    this.fetch = config.fetch ?? globalThis.fetch;
    this.retryConfig = {
      baseDelayMs: config.retry?.baseDelayMs ?? DEFAULT_RETRY_BASE_DELAY_MS,
      maxDelayMs: config.retry?.maxDelayMs ?? DEFAULT_RETRY_MAX_DELAY_MS,
      heartbeatTimeoutMs: config.retry?.heartbeatTimeoutMs ?? DEFAULT_HEARTBEAT_TIMEOUT_MS,
      enabled: config.retry?.enabled ?? true,
    };
    this.analyticsConfig = {
      enabled: config.analytics?.enabled ?? true,
      flushIntervalMs: Math.max(
        MIN_FLUSH_INTERVAL_MS,
        config.analytics?.flushIntervalMs ?? DEFAULT_FLUSH_INTERVAL_MS,
      ),
      maxBatchSize: config.analytics?.maxBatchSize ?? DEFAULT_MAX_BATCH_SIZE,
      maxBufferSize: config.analytics?.maxBufferSize ?? DEFAULT_MAX_BUFFER_SIZE,
      samplingRate: Math.min(1, Math.max(0, config.analytics?.samplingRate ?? 1)),
    };
    if (this.analyticsConfig.enabled) this.startAnalyticsTimer();
  }

  /**
   * Synchronous read from the local cache populated by `evaluateAll()` or `stream()`.
   * Returns `defaultValue` if the flag has not been evaluated yet.
   */
  get<T extends FlagValue>(flagKey: string, defaultValue: T): T {
    const startedAt = nowMs();
    const hit = this.cache[flagKey];
    const value = hit ? (hit.value as T) : defaultValue;
    this.recordEvaluation({
      flagKey,
      variationKey: variationKeyFromValue(value as FlagValue),
      reason: hit ? hit.reason : 'flag_not_found',
      latencyMs: nowMs() - startedAt,
      timestamp: Math.floor(Date.now() / 1000),
    });
    return value;
  }

  /**
   * Snapshot of the current local flag cache.
   */
  getAll(): Record<string, EvaluationResult> {
    return { ...this.cache };
  }

  /**
   * Evaluate a single feature flag for a user context.
   *
   * Single network attempt — this sits on the caller's request path, so it
   * never retries. On any failure other than a definitive 404 it serves the
   * last-known-good cached value (hydrated by `evaluateAll()`/`stream()`)
   * before falling back to `defaultValue`. That includes 401/403: an auth
   * error is never allowed to degrade a value the SDK already holds.
   */
  async evaluate<T extends FlagValue>(flagKey: string, context: UserContext, defaultValue: T): Promise<T> {
    const startedAt = nowMs();
    try {
      const res = await this.fetch(`${this.apiUrl}/v1/evaluate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': this.apiKey },
        body: JSON.stringify({ flagKey, context }),
      });
      if (!res.ok) {
        if (res.status === 401 || res.status === 403) {
          this.warnDeduped(
            'auth:evaluate',
            `[ShipSilently] Authentication failed (${res.status}) for flag "${flagKey}". Serving last-known-good values. Check your API key if this persists.`,
          );
        }
        if (res.status === 404) {
          // Definitive answer: the flag doesn't exist. The server records
          // nothing for a failed evaluation, so this locally-served default
          // is a flag check only the client can count.
          this.recordEvaluation({
            flagKey,
            variationKey: variationKeyFromValue(defaultValue as FlagValue),
            reason: 'flag_not_found',
            latencyMs: nowMs() - startedAt,
            timestamp: Math.floor(Date.now() / 1000),
          });
          return defaultValue;
        }
        return this.serveStale(flagKey, defaultValue, startedAt);
      }
      const data = await res.json() as EvaluationResult;
      // Cache the success so a later failure (outage, 401) can serve this as
      // last-known-good — an app that only ever calls evaluate() must still
      // build a cache, otherwise it degrades straight to defaults during an
      // incident, the exact regression this SDK exists to prevent.
      this.cache[flagKey] = data;
      // No client event on success: the server already recorded this
      // evaluation (source=server). Recording it here too would double-count
      // every networked check on the dashboard.
      return data.value as T;
    } catch {
      return this.serveStale(flagKey, defaultValue, startedAt);
    }
  }

  /**
   * Serve the last-known-good cached value for a flag (or `defaultValue` when
   * the cache has never been hydrated) after a failed evaluation. Recorded as
   * `error_fallback` — the flag may exist, the request just didn't succeed.
   */
  private serveStale<T extends FlagValue>(flagKey: string, defaultValue: T, startedAt: number): T {
    const hit = this.cache[flagKey];
    const value = hit ? (hit.value as T) : defaultValue;
    this.recordEvaluation({
      flagKey,
      variationKey: variationKeyFromValue(value as FlagValue),
      reason: 'error_fallback',
      latencyMs: nowMs() - startedAt,
      timestamp: Math.floor(Date.now() / 1000),
    });
    return value;
  }

  /**
   * Evaluate all flags for a user context in a single network call.
   *
   * The local cache is only ever replaced by a successful response. On any
   * failure — network, 5xx, and explicitly 401/403 — the last-known-good
   * cache is returned untouched, so subscribers never regress to an empty
   * flag map because the control plane had a bad moment. Single attempt: the
   * poll/reconnect loop in `stream()` is this call's retry cadence.
   */
  async evaluateAll(context: UserContext): Promise<Record<string, EvaluationResult>> {
    try {
      const res = await this.fetch(`${this.apiUrl}/v1/evaluate/batch`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': this.apiKey },
        body: JSON.stringify({ context }),
      });
      if (!res.ok) {
        if (res.status === 401 || res.status === 403) {
          this.warnDeduped(
            'auth:evaluateAll',
            `[ShipSilently] Authentication failed (${res.status}) for evaluateAll. Serving last-known-good values. Check your API key if this persists.`,
          );
        }
        return { ...this.cache };
      }
      const data = await res.json() as { flags: Record<string, EvaluationResult> };
      this.cache = data.flags;
      // No client events here: the server records one event per flag for the
      // batch (source=server). Cache reads served from this hydration are
      // counted by get() as they happen.
      return data.flags;
    } catch {
      return { ...this.cache };
    }
  }

  /**
   * Subscribe to real-time flag updates via SSE (Pro plan+).
   *
   * The server emits a `flags.updated` event whenever any flag in the env
   * changes. The SDK reacts by re-running `evaluateAll(context)` to refresh
   * the local cache, then invokes `onChange` with the new flag map.
   *
   * The subscription has no terminal failure state. On any stream error —
   * including auth errors (401/403), which can be transient (key rotation
   * races, control-plane incidents) — the SDK polls `evaluateAll` to keep
   * data fresh while reconnecting with full-jitter exponential backoff.
   * It never gives up and never requires an application restart. The only
   * cases that settle into polling-only are a 402 feature gate (free plan),
   * a runtime without `EventSource`, or `retry.enabled: false`.
   *
   * A server `heartbeat` event every 30s keeps the connection observable; a
   * watchdog reconnects when the stream has been silent past
   * `retry.heartbeatTimeoutMs`.
   *
   * Returns an `unsubscribe` function — call it to close the connection.
   */
  stream(
    context: UserContext,
    onChange: FlagsUpdateCallback,
    options: {
      pollingIntervalMs?: number;
      onStateChange?: (state: StreamConnectionState) => void;
    } = {},
  ): () => void {
    const pollingMs = options.pollingIntervalMs ?? 30_000;
    const retry = this.retryConfig;

    let es: EventSource | null = null;
    let pollTimer: ReturnType<typeof setInterval> | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let watchdogTimer: ReturnType<typeof setInterval> | null = null;
    let healthyTimer: ReturnType<typeof setTimeout> | null = null;
    let closed = false;
    let attempt = 0;
    let lastEventAt = 0;
    let state: StreamConnectionState | null = null;
    let hadFailure = false;

    const setState = (next: StreamConnectionState) => {
      if (closed || state === next) return;
      state = next;
      options.onStateChange?.(next);
    };

    const refresh = async () => {
      if (closed) return;
      const flags = await this.evaluateAll(context);
      if (!closed) onChange(flags);
    };

    const startPolling = () => {
      if (pollTimer || closed) return;
      pollTimer = setInterval(refresh, pollingMs);
      unrefTimer(pollTimer);
    };

    const stopPolling = () => {
      if (!pollTimer) return;
      clearInterval(pollTimer);
      pollTimer = null;
    };

    const teardownStream = () => {
      es?.close();
      es = null;
      if (watchdogTimer) {
        clearInterval(watchdogTimer);
        watchdogTimer = null;
      }
      if (healthyTimer) {
        clearTimeout(healthyTimer);
        healthyTimer = null;
      }
    };

    // Terminal polling mode: feature gate (402), no EventSource in this
    // runtime, or retries disabled. Not a failure state — data stays fresh.
    const pollOnly = () => {
      void refresh();
      startPolling();
      setState('polling');
    };

    const scheduleReconnect = () => {
      if (closed) return;
      hadFailure = true;
      if (!retry.enabled) {
        // Legacy behavior: downgrade to polling for the subscription's life.
        pollOnly();
        return;
      }
      // Poll while disconnected so flag data stays fresh between attempts.
      void refresh();
      startPolling();
      setState('reconnecting');
      // Full jitter: delay ∈ [0, min(cap, base·2^attempt)). Jitter spreads a
      // fleet's reconnects out so recovery doesn't become a thundering herd.
      const ceiling = Math.min(retry.maxDelayMs, retry.baseDelayMs * 2 ** Math.min(attempt, 30));
      attempt += 1;
      const delay = Math.floor(Math.random() * ceiling);
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        void connect();
      }, delay);
      unrefTimer(reconnectTimer);
    };

    const startWatchdog = () => {
      const checkMs = Math.max(1_000, Math.floor(retry.heartbeatTimeoutMs / 3));
      watchdogTimer = setInterval(() => {
        if (!es || Date.now() - lastEventAt <= retry.heartbeatTimeoutMs) return;
        // The server heartbeats every 30s; silence past the timeout means the
        // socket died without an error event (e.g. dropped by a middlebox).
        this.warnDeduped(
          'stream:stale',
          '[ShipSilently] Stream silent past heartbeat timeout — reconnecting.',
        );
        teardownStream();
        scheduleReconnect();
      }, checkMs);
      unrefTimer(watchdogTimer);
    };

    const connect = async () => {
      if (closed) return;
      if (typeof EventSource === 'undefined') {
        // No SSE in this runtime. Polling plus the never-cleared-on-error
        // cache keeps this mode restart-free too.
        pollOnly();
        return;
      }

      // EventSource can't send custom headers, so we can't put the API key on
      // the connection directly — and it must never go on the URL as a query
      // string (it would leak into logs, proxy caches and browser history).
      // Exchange the header-authenticated key for a single-use ticket and put
      // *that* on the EventSource URL instead. The ticket is fetched *after*
      // any backoff wait (never before) so its 60s TTL can't lapse mid-sleep.
      const ticket = await this.fetchStreamTicket();
      if (closed) return;

      if (!ticket.ok) {
        if (ticket.status === 402) {
          // Streaming isn't in this plan — a feature gate, not an outage.
          // Poll; don't retry SSE against a door that's closed on purpose.
          pollOnly();
          return;
        }
        if (ticket.status === 401 || ticket.status === 403) {
          this.warnDeduped(
            'auth:stream',
            `[ShipSilently] Stream auth failed (${ticket.status}). Retrying with backoff — auth errors can be transient (key rotation, control-plane incidents) and are never treated as fatal. Check your API key if this persists.`,
          );
        }
        scheduleReconnect();
        return;
      }

      const url = `${this.apiUrl}/v1/stream?ticket=${encodeURIComponent(ticket.ticket)}`;
      const self = new EventSource(url);
      es = self;
      lastEventAt = Date.now();
      startWatchdog();

      // Prime the cache as soon as the connection is open so subscribers
      // have flag state at t=0 without waiting for the first mutation.
      self.addEventListener('connected', () => {
        lastEventAt = Date.now();
        stopPolling();
        setState('streaming');
        if (hadFailure) {
          // Deduped like the warnings, so a flapping connection doesn't spam
          // one recovery line per reconnect.
          this.warnDeduped('stream:recovered', '[ShipSilently] Stream connection recovered.', console.info);
          hadFailure = false;
        }
        // Reset the backoff ladder only once the connection proves healthy.
        healthyTimer = setTimeout(() => {
          attempt = 0;
        }, HEALTHY_CONNECTION_RESET_MS);
        unrefTimer(healthyTimer);
        void refresh();
      });

      self.addEventListener('flags.updated', () => {
        lastEventAt = Date.now();
        void refresh();
      });

      self.addEventListener('heartbeat', () => {
        lastEventAt = Date.now();
      });

      // Guard against a late error from a torn-down EventSource: once we've
      // replaced `es` with a newer connection (or closed), an old socket's
      // queued error must not tear down the live one.
      self.onerror = () => {
        if (closed || es !== self) return;
        teardownStream();
        scheduleReconnect();
      };
    };

    void connect();

    return () => {
      closed = true;
      teardownStream();
      stopPolling();
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
    };
  }

  /**
   * Exchange the API key (sent in the X-API-Key header) for a single-use,
   * short-lived stream ticket. The ticket — not the key — goes on the
   * EventSource URL, keeping the standing secret out of logs and browser
   * history. Failures preserve the HTTP status so the caller can distinguish
   * a feature gate (402 → poll) from an auth error or outage (retry).
   */
  private async fetchStreamTicket(): Promise<
    { ok: true; ticket: string } | { ok: false; status: number | null }
  > {
    try {
      const res = await this.fetch(`${this.apiUrl}/v1/stream/ticket`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': this.apiKey },
      });
      if (!res.ok) return { ok: false, status: res.status };
      const data = (await res.json()) as { ticket?: string };
      return data.ticket ? { ok: true, ticket: data.ticket } : { ok: false, status: null };
    } catch {
      return { ok: false, status: null };
    }
  }

  /**
   * Log at most once per `WARN_DEDUP_INTERVAL_MS` per key. State transitions
   * get one line; a flapping connection repeats at most once a minute instead
   * of once per retry. `log` defaults to `console.warn`; the recovery line
   * passes `console.info`.
   */
  private warnDeduped(key: string, message: string, log: (msg: string) => void = console.warn): void {
    const now = Date.now();
    if (now - (this.lastWarnAt.get(key) ?? 0) < WARN_DEDUP_INTERVAL_MS) return;
    this.lastWarnAt.set(key, now);
    log(message);
  }

  // ─── Analytics ──────────────────────────────────────────────────────────────

  /**
   * Force-flush the analytics buffer. Returns the number of events submitted
   * (0 when the buffer was already empty or analytics are disabled).
   *
   * Tests should call this and `await` the result instead of relying on the
   * timer. Production callers may invoke it on `beforeunload` to ensure the
   * final batch is sent before the page unloads.
   */
  async flushAnalytics(): Promise<number> {
    if (!this.analyticsConfig.enabled) return 0;
    if (this.analyticsBuffer.length === 0) return 0;
    const pending = this.analyticsBuffer;
    this.analyticsBuffer = [];

    // The server rejects batches above MAX_EVENTS_PER_REQUEST with a hard 400,
    // so a large buffer (e.g. re-buffered during an outage) must drain in
    // sequential chunks. A failed chunk stops the flush; that chunk and every
    // unsent event after it go back on the buffer (when retryable) so the next
    // flush picks up where this one left off.
    let sent = 0;
    for (let offset = 0; offset < pending.length; offset += MAX_EVENTS_PER_REQUEST) {
      const chunk = pending.slice(offset, offset + MAX_EVENTS_PER_REQUEST);
      let retryable: boolean;
      try {
        const res = await this.fetch(`${this.apiUrl}/v1/analytics/events`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-API-Key': this.apiKey },
          body: JSON.stringify({ events: chunk }),
        });
        if (res.ok) {
          sent += chunk.length;
          continue;
        }
        // Server errors (>=500) and throttling (429) are retryable: dropping
        // them silently loses metrics and leaves the dashboard empty. Hard
        // 4xx (400/401/403 — malformed batch or bad credentials) are NOT
        // retried, since replaying them can't succeed and would grow the
        // buffer unbounded.
        retryable = res.status >= 500 || res.status === 429;
      } catch {
        // Network error — always retryable.
        retryable = true;
      }
      if (retryable) {
        // Re-buffering is capped so a permanently-failing server can't cause
        // unbounded growth.
        const unsent = pending.slice(offset);
        if (this.analyticsBuffer.length + unsent.length <= this.analyticsConfig.maxBufferSize) {
          this.analyticsBuffer = [...unsent, ...this.analyticsBuffer];
        }
      }
      break;
    }
    return sent;
  }

  /** Stop the periodic analytics flush. Use before discarding the client. */
  close(): void {
    if (this.analyticsTimer) {
      clearInterval(this.analyticsTimer);
      this.analyticsTimer = null;
    }
    // Best-effort final flush — fire and forget.
    void this.flushAnalytics();
  }

  /** @internal */
  recordEvaluation(event: PendingAnalyticsEvent): void {
    if (!this.analyticsConfig.enabled) return;
    if (this.analyticsConfig.samplingRate < 1 && Math.random() >= this.analyticsConfig.samplingRate) {
      return;
    }
    if (this.analyticsBuffer.length >= this.analyticsConfig.maxBufferSize) {
      // Drop oldest to keep memory bounded — recent samples are more useful.
      this.analyticsBuffer.shift();
    }
    this.analyticsBuffer.push(event);
    if (this.analyticsBuffer.length >= this.analyticsConfig.maxBatchSize) {
      void this.flushAnalytics();
    }
  }

  /** @internal — exposed for tests; reset mid-flight. */
  _bufferSize(): number {
    return this.analyticsBuffer.length;
  }

  private startAnalyticsTimer(): void {
    if (typeof setInterval === 'undefined') return;
    this.analyticsTimer = setInterval(() => {
      void this.flushAnalytics();
    }, this.analyticsConfig.flushIntervalMs);
    unrefTimer(this.analyticsTimer);
  }
}

// In Node-like environments, setInterval/setTimeout keep the event loop
// alive. Use unref() where available so SDK timers never block process exit.
function unrefTimer(timer: unknown): void {
  const t = timer as { unref?: () => void } | null;
  if (t && typeof t.unref === 'function') t.unref();
}

function nowMs(): number {
  if (typeof performance !== 'undefined' && typeof performance.now === 'function') {
    return performance.now();
  }
  return Date.now();
}

/**
 * Create a ShipSilently client instance.
 *
 * @example
 * ```ts
 * import ShipSilently from '@shipsilently/node';
 *
 * const client = ShipSilently.init({ apiKey: 'sk_live_...' });
 * const enabled = await client.evaluate('new-checkout', { userId: 'u_123' }, false);
 * ```
 */
const ShipSilently = {
  init(config: ShipSilentlyConfig): ShipSilentlyClient {
    return new ShipSilentlyClient(config);
  },
};

export default ShipSilently;
export { ShipSilently };
