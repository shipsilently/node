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

export interface ShipSilentlyConfig {
  apiKey: string;
  /** Defaults to the production ShipSilently API */
  apiUrl?: string;
  /** Override the fetch implementation — useful for testing */
  fetch?: typeof globalThis.fetch;
  /** Analytics submission settings — see AnalyticsConfig. */
  analytics?: AnalyticsConfig;
}

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

  constructor(config: ShipSilentlyConfig) {
    this.apiKey = config.apiKey;
    this.apiUrl = (config.apiUrl ?? DEFAULT_API_URL).replace(/\/$/, '');
    this.fetch = config.fetch ?? globalThis.fetch;
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
   * Returns `defaultValue` if the flag is not found or a network error occurs.
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
          console.warn(
            `[ShipSilently] Authentication failed (${res.status}) for flag "${flagKey}". Check your API key.`,
          );
        }
        // The server records nothing for a failed evaluation, so this default
        // that the SDK served locally is a flag check only the client can
        // count. 404 keeps the server's `flag_not_found` reason; other
        // failures (429/5xx) are `error_fallback` — the flag may exist, the
        // request just didn't succeed.
        this.recordEvaluation({
          flagKey,
          variationKey: variationKeyFromValue(defaultValue as FlagValue),
          reason: res.status === 404 ? 'flag_not_found' : 'error_fallback',
          latencyMs: nowMs() - startedAt,
          timestamp: Math.floor(Date.now() / 1000),
        });
        return defaultValue;
      }
      const data = await res.json() as EvaluationResult;
      // No client event on success: the server already recorded this
      // evaluation (source=server). Recording it here too would double-count
      // every networked check on the dashboard.
      return data.value as T;
    } catch {
      this.recordEvaluation({
        flagKey,
        variationKey: variationKeyFromValue(defaultValue as FlagValue),
        reason: 'error_fallback',
        latencyMs: nowMs() - startedAt,
        timestamp: Math.floor(Date.now() / 1000),
      });
      return defaultValue;
    }
  }

  /**
   * Evaluate all flags for a user context in a single network call.
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
          console.warn(
            `[ShipSilently] Authentication failed (${res.status}) for evaluateAll. Check your API key.`,
          );
        }
        return {};
      }
      const data = await res.json() as { flags: Record<string, EvaluationResult> };
      this.cache = data.flags;
      // No client events here: the server records one event per flag for the
      // batch (source=server). Cache reads served from this hydration are
      // counted by get() as they happen.
      return data.flags;
    } catch {
      return {};
    }
  }

  /**
   * Subscribe to real-time flag updates via SSE (Pro plan+).
   *
   * The server emits a `flags.updated` event whenever any flag in the env
   * changes. The SDK reacts by re-running `evaluateAll(context)` to refresh
   * the local cache, then invokes `onChange` with the new flag map.
   *
   * Falls back to polling `evaluateAll` on EventSource failure or when
   * `EventSource` is unavailable (e.g. server-side runtimes).
   *
   * Returns an `unsubscribe` function — call it to close the connection.
   */
  stream(
    context: UserContext,
    onChange: FlagsUpdateCallback,
    options: { pollingIntervalMs?: number } = {},
  ): () => void {
    const pollingMs = options.pollingIntervalMs ?? 30_000;

    let es: EventSource | null = null;
    let pollTimer: ReturnType<typeof setInterval> | null = null;
    let closed = false;

    const refresh = async () => {
      if (closed) return;
      const flags = await this.evaluateAll(context);
      if (!closed) onChange(flags);
    };

    const startPolling = () => {
      if (pollTimer || closed) return;
      pollTimer = setInterval(refresh, pollingMs);
    };

    const tryStream = async () => {
      if (typeof EventSource === 'undefined') {
        // Prime the cache, then poll on an interval
        void refresh();
        startPolling();
        return;
      }

      // EventSource can't send custom headers, so we can't put the API key on
      // the connection directly — and it must never go on the URL as a query
      // string (it would leak into logs, proxy caches and browser history).
      // Exchange the header-authenticated key for a single-use ticket and put
      // *that* on the EventSource URL instead.
      const ticket = await this.fetchStreamTicket();
      if (closed) return;
      if (!ticket) {
        // No ticket (network error, free plan, etc.) — fall back to polling.
        void refresh();
        startPolling();
        return;
      }

      const url = `${this.apiUrl}/v1/stream?ticket=${encodeURIComponent(ticket)}`;
      es = new EventSource(url);

      // Prime the cache as soon as the connection is open so subscribers
      // have flag state at t=0 without waiting for the first mutation.
      es.addEventListener('connected', () => {
        void refresh();
      });

      es.addEventListener('flags.updated', () => {
        void refresh();
      });

      es.onerror = () => {
        es?.close();
        es = null;
        // Fall back to polling on SSE error (e.g. free plan 402)
        void refresh();
        startPolling();
      };
    };

    void tryStream();

    return () => {
      closed = true;
      es?.close();
      if (pollTimer) clearInterval(pollTimer);
    };
  }

  /**
   * Exchange the API key (sent in the X-API-Key header) for a single-use,
   * short-lived stream ticket. Returns null on any failure so the caller can
   * fall back to polling. The ticket — not the key — goes on the EventSource
   * URL, keeping the standing secret out of logs and browser history.
   */
  private async fetchStreamTicket(): Promise<string | null> {
    try {
      const res = await this.fetch(`${this.apiUrl}/v1/stream/ticket`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-API-Key': this.apiKey },
      });
      if (!res.ok) return null;
      const data = (await res.json()) as { ticket?: string };
      return data.ticket ?? null;
    } catch {
      return null;
    }
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
    // In Node-like environments, setInterval keeps the event loop alive. Use
    // unref() if available so a hanging timer doesn't block process exit.
    const t = this.analyticsTimer as { unref?: () => void } | null;
    if (t && typeof t.unref === 'function') t.unref();
  }
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
