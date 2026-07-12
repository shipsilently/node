import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { ShipSilentlyClient, type RetryConfig, type StreamConnectionState } from '../src/index';

const mockFetch = vi.fn();
const client = new ShipSilentlyClient({
  apiKey: 'test-key',
  apiUrl: 'http://localhost:8787',
  fetch: mockFetch as typeof globalThis.fetch,
});

beforeEach(() => mockFetch.mockReset());

describe('ShipSilentlyClient.evaluate', () => {
  it('returns the flag value on success', async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: async () => ({ flagKey: 'my-flag', value: true, reason: 'rule_match' }),
    });
    const val = await client.evaluate('my-flag', { userId: 'u1' }, false);
    expect(val).toBe(true);
    expect(mockFetch).toHaveBeenCalledWith(
      'http://localhost:8787/v1/evaluate',
      expect.objectContaining({ method: 'POST' }),
    );
  });

  it('returns defaultValue when API returns non-ok status and nothing is cached', async () => {
    mockFetch.mockResolvedValue({ ok: false, status: 401 } as Response);
    // Uncached flag key → no last-known-good to serve → caller's default.
    const val = await client.evaluate('uncached-401', { userId: 'u1' }, 'fallback');
    expect(val).toBe('fallback');
  });

  it('returns defaultValue on fetch failure (non-ok 500) when nothing is cached', async () => {
    mockFetch.mockResolvedValue({ ok: false, status: 500 } as Response);
    const val = await client.evaluate('uncached-500', {}, 42);
    expect(val).toBe(42);
  });

  it('serves the cached value before defaultValue when evaluation fails', async () => {
    const fetchMock = vi.fn();
    const c = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost:8787',
      fetch: fetchMock as unknown as typeof globalThis.fetch,
      analytics: { enabled: false },
    });
    fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({
        flags: { 'cached-flag': { flagKey: 'cached-flag', value: 'blue', reason: 'rule_match' } },
      }),
    });
    await c.evaluateAll({});

    // 500, 401, and network throw all fall back to last-known-good.
    fetchMock.mockResolvedValue({ ok: false, status: 500 } as Response);
    expect(await c.evaluate('cached-flag', {}, 'red')).toBe('blue');
    fetchMock.mockResolvedValue({ ok: false, status: 401 } as Response);
    expect(await c.evaluate('cached-flag', {}, 'red')).toBe('blue');
    fetchMock.mockRejectedValue(new Error('network down'));
    expect(await c.evaluate('cached-flag', {}, 'red')).toBe('blue');

    // A flag that was never cached still gets the caller's default.
    fetchMock.mockResolvedValue({ ok: false, status: 500 } as Response);
    expect(await c.evaluate('never-seen', {}, 'red')).toBe('red');

    // 404 is a definitive answer, not an outage — default wins over cache.
    fetchMock.mockResolvedValue({ ok: false, status: 404 } as Response);
    expect(await c.evaluate('cached-flag', {}, 'red')).toBe('red');
  });

  it('caches a successful evaluate() so an evaluate-only app survives a later 401', async () => {
    const fetchMock = vi.fn();
    const c = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost:8787',
      fetch: fetchMock as unknown as typeof globalThis.fetch,
      analytics: { enabled: false },
    });
    // App only ever calls evaluate() — never evaluateAll()/stream().
    fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({ flagKey: 'kill-switch', value: 'on', reason: 'rule_match' }),
    });
    expect(await c.evaluate('kill-switch', {}, 'off')).toBe('on');

    // Now an outage 401s everything. The value the SDK already held must not
    // degrade to the default.
    fetchMock.mockResolvedValue({ ok: false, status: 401 } as Response);
    expect(await c.evaluate('kill-switch', {}, 'off')).toBe('on');
    expect(c.get('kill-switch', 'off')).toBe('on');
  });
});

describe('ShipSilentlyClient.evaluateAll', () => {
  it('returns flags map on success', async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        flags: {
          'flag-a': { flagKey: 'flag-a', value: true, reason: 'default' },
        },
      }),
    });
    const flags = await client.evaluateAll({ userId: 'u1' });
    expect(flags['flag-a']?.value).toBe(true);
  });

  it('returns the last-known-good cache on failure, and empty when never hydrated', async () => {
    const fetchMock = vi.fn();
    const c = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost:8787',
      fetch: fetchMock as unknown as typeof globalThis.fetch,
      analytics: { enabled: false },
    });

    // Never hydrated → nothing to serve.
    fetchMock.mockResolvedValue({ ok: false, status: 500 } as Response);
    expect(await c.evaluateAll({})).toEqual({});

    // Hydrate, then fail — the cache must survive untouched.
    fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({
        flags: { 'flag-a': { flagKey: 'flag-a', value: true, reason: 'default' } },
      }),
    });
    await c.evaluateAll({});
    fetchMock.mockResolvedValue({ ok: false, status: 500 } as Response);
    const flags = await c.evaluateAll({});
    expect(flags['flag-a']?.value).toBe(true);
    expect(c.get('flag-a', false)).toBe(true);
  });

  it('keeps serving last-known-good values after a 401 — auth errors never clear the cache', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => void 0);
    const fetchMock = vi.fn();
    const c = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost:8787',
      fetch: fetchMock as unknown as typeof globalThis.fetch,
      analytics: { enabled: false },
    });

    fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({
        flags: { 'kill-switch': { flagKey: 'kill-switch', value: 'on', reason: 'rule_match' } },
      }),
    });
    await c.evaluateAll({});

    fetchMock.mockResolvedValue({ ok: false, status: 401 } as Response);
    const during401 = await c.evaluateAll({});
    expect(during401['kill-switch']?.value).toBe('on');
    expect(c.get('kill-switch', 'off')).toBe('on');

    // Repeated 401s log once, not once per call.
    await c.evaluateAll({});
    await c.evaluateAll({});
    expect(warnSpy).toHaveBeenCalledTimes(1);
    warnSpy.mockRestore();
  });

  it('populates the local cache so get() returns evaluated values', async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        flags: {
          'flag-a': { flagKey: 'flag-a', value: true, reason: 'default' },
          'flag-b': { flagKey: 'flag-b', value: 'green', reason: 'rule_match' },
        },
      }),
    });
    await client.evaluateAll({ userId: 'u1' });
    expect(client.get('flag-a', false)).toBe(true);
    expect(client.get('flag-b', 'fallback')).toBe('green');
    expect(client.get('flag-missing', 42)).toBe(42);
  });
});

// Minimal fake EventSource to drive the SDK's stream() behavior in tests.
class FakeEventSource {
  static instances: FakeEventSource[] = [];
  url: string;
  onerror: ((ev: Event) => void) | null = null;
  private listeners = new Map<string, Set<(ev: MessageEvent) => void>>();
  closed = false;

  constructor(url: string) {
    this.url = url;
    FakeEventSource.instances.push(this);
  }

  addEventListener(event: string, fn: (ev: MessageEvent) => void): void {
    const bucket = this.listeners.get(event) ?? new Set();
    bucket.add(fn);
    this.listeners.set(event, bucket);
  }

  emit(event: string, data: string): void {
    const bucket = this.listeners.get(event);
    if (!bucket) return;
    const ev = { data } as MessageEvent;
    for (const fn of bucket) fn(ev);
  }

  close(): void {
    this.closed = true;
  }
}

describe('ShipSilentlyClient.stream', () => {
  const originalEventSource = (globalThis as { EventSource?: unknown }).EventSource;

  beforeEach(() => {
    FakeEventSource.instances = [];
    (globalThis as { EventSource?: unknown }).EventSource = FakeEventSource as unknown as typeof EventSource;
  });

  afterEach(() => {
    (globalThis as { EventSource?: unknown }).EventSource = originalEventSource;
  });

  // Route the ticket exchange to a stub ticket and everything else to the
  // flags batch response, mirroring the real ticket → EventSource flow.
  function mockTicketAnd(flags: Record<string, unknown>) {
    mockFetch.mockImplementation((url: unknown) => {
      if (String(url).endsWith('/v1/stream/ticket')) {
        return Promise.resolve({ ok: true, json: async () => ({ ticket: 'tkt_test', expiresIn: 60 }) });
      }
      return Promise.resolve({ ok: true, json: async () => ({ flags }) });
    });
  }

  it('refreshes the cache and invokes onChange when a flags.updated event arrives', async () => {
    mockTicketAnd({ 'flag-a': { flagKey: 'flag-a', value: true, reason: 'default' } });

    const onChange = vi.fn();
    const unsubscribe = client.stream({ userId: 'u1' }, onChange);

    // The EventSource is opened only after the async ticket exchange resolves.
    await vi.waitFor(() => expect(FakeEventSource.instances.length).toBe(1));
    const es = FakeEventSource.instances[0]!;
    expect(es.url).toContain('/v1/stream?ticket=');
    expect(es.url).not.toContain('apiKey');

    // Server signals a flag change with just { envId }
    es.emit('flags.updated', JSON.stringify({ envId: 'env_123' }));

    // Allow the async evaluateAll() inside the handler to resolve
    await vi.waitFor(() => expect(onChange).toHaveBeenCalled());

    expect(mockFetch).toHaveBeenCalledWith(
      'http://localhost:8787/v1/evaluate/batch',
      expect.objectContaining({ method: 'POST' }),
    );
    expect(client.get('flag-a', false)).toBe(true);

    unsubscribe();
    expect(es.closed).toBe(true);
  });

  it('primes the cache on the connected event', async () => {
    mockTicketAnd({ 'priming': { flagKey: 'priming', value: 'hello', reason: 'default' } });

    const onChange = vi.fn();
    const unsubscribe = client.stream({}, onChange);

    await vi.waitFor(() => expect(FakeEventSource.instances.length).toBe(1));
    const es = FakeEventSource.instances[0]!;
    es.emit('connected', JSON.stringify({ connectionId: 'abc' }));

    await vi.waitFor(() => expect(client.get('priming', '')).toBe('hello'));
    expect(onChange).toHaveBeenCalled();

    unsubscribe();
  });

  it('falls back to polling when EventSource is unavailable', async () => {
    (globalThis as { EventSource?: unknown }).EventSource = undefined;

    mockFetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        flags: { polled: { flagKey: 'polled', value: 7, reason: 'default' } },
      }),
    });

    const onChange = vi.fn();
    const unsubscribe = client.stream({}, onChange, { pollingIntervalMs: 10_000 });

    // Initial prime call
    await vi.waitFor(() => expect(onChange).toHaveBeenCalled());
    expect(client.get('polled', 0)).toBe(7);

    unsubscribe();
  });

  it('falls back to polling (no EventSource) when the ticket exchange fails', async () => {
    mockFetch.mockImplementation((url: unknown) => {
      if (String(url).endsWith('/v1/stream/ticket')) {
        return Promise.resolve({ ok: false, status: 402 });
      }
      return Promise.resolve({
        ok: true,
        json: async () => ({ flags: { gated: { flagKey: 'gated', value: 1, reason: 'default' } } }),
      });
    });

    const onChange = vi.fn();
    const unsubscribe = client.stream({}, onChange, { pollingIntervalMs: 10_000 });

    // No ticket → never opens an EventSource, just polls.
    await vi.waitFor(() => expect(onChange).toHaveBeenCalled());
    expect(FakeEventSource.instances.length).toBe(0);
    expect(client.get('gated', 0)).toBe(1);

    unsubscribe();
  });
});

// ─── Auto-recovery: no terminal states, full-jitter backoff, watchdog ────────
//
// The policy under test: any stream failure — explicitly including 401/403 —
// polls to keep data fresh while reconnecting with full-jitter exponential
// backoff, forever. Only a 402 feature gate, a missing EventSource, or
// retry.enabled:false settle into polling-only.
describe('ShipSilentlyClient.stream auto-recovery', () => {
  const originalEventSource = (globalThis as { EventSource?: unknown }).EventSource;
  const FLAGS = { 'flag-a': { flagKey: 'flag-a', value: true, reason: 'default' } };

  beforeEach(() => {
    FakeEventSource.instances = [];
    (globalThis as { EventSource?: unknown }).EventSource =
      FakeEventSource as unknown as typeof EventSource;
    vi.useFakeTimers();
    vi.spyOn(console, 'warn').mockImplementation(() => void 0);
    vi.spyOn(console, 'info').mockImplementation(() => void 0);
  });

  afterEach(() => {
    (globalThis as { EventSource?: unknown }).EventSource = originalEventSource;
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  function makeClient(retry?: RetryConfig) {
    const fetchMock = vi.fn();
    const c = new ShipSilentlyClient({
      apiKey: 'test-key',
      apiUrl: 'http://localhost:8787',
      fetch: fetchMock as unknown as typeof globalThis.fetch,
      analytics: { enabled: false },
      retry,
    });
    return { c, fetchMock };
  }

  /** fetch mock routing tickets through `ticketImpl` and batches to FLAGS. */
  function routeFetch(
    fetchMock: ReturnType<typeof vi.fn>,
    ticketImpl: () => { ok: boolean; status?: number },
    counters: { tickets: number; batches: number },
  ) {
    fetchMock.mockImplementation((url: unknown) => {
      if (String(url).endsWith('/v1/stream/ticket')) {
        counters.tickets += 1;
        const t = ticketImpl();
        return Promise.resolve(
          t.ok
            ? { ok: true, json: async () => ({ ticket: 'tkt_test', expiresIn: 60 }) }
            : { ok: false, status: t.status ?? 500 },
        );
      }
      counters.batches += 1;
      return Promise.resolve({ ok: true, json: async () => ({ flags: FLAGS }) });
    });
  }

  it('reconnects after a stream error and stops polling once streaming again', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    const { c, fetchMock } = makeClient();
    const counters = { tickets: 0, batches: 0 };
    routeFetch(fetchMock, () => ({ ok: true }), counters);

    const states: StreamConnectionState[] = [];
    const unsubscribe = c.stream({}, () => void 0, {
      pollingIntervalMs: 10_000,
      onStateChange: (s) => states.push(s),
    });

    await vi.advanceTimersByTimeAsync(0);
    expect(FakeEventSource.instances.length).toBe(1);
    const es1 = FakeEventSource.instances[0]!;
    es1.emit('connected', '{}');
    await vi.advanceTimersByTimeAsync(0);
    expect(states[states.length - 1]).toBe('streaming');

    // Kill the stream. The SDK must start polling AND schedule a reconnect.
    es1.onerror?.(new Event('error'));
    await vi.advanceTimersByTimeAsync(0);
    expect(es1.closed).toBe(true);
    expect(states[states.length - 1]).toBe('reconnecting');

    // Full jitter with r=0.5 and attempt=0 → delay 500ms. No reconnect yet
    // at 499ms; a fresh ticket is fetched and a NEW EventSource opens at 500.
    await vi.advanceTimersByTimeAsync(499);
    expect(FakeEventSource.instances.length).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    await vi.advanceTimersByTimeAsync(0);
    expect(FakeEventSource.instances.length).toBe(2);

    const es2 = FakeEventSource.instances[1]!;
    es2.emit('connected', '{}');
    await vi.advanceTimersByTimeAsync(0);
    expect(states[states.length - 1]).toBe('streaming');

    // Polling must stop while streaming: over the next 30s (3 poll periods)
    // no batch refreshes beyond the connected-prime happen.
    const batchesAfterConnect = counters.batches;
    await vi.advanceTimersByTimeAsync(30_000);
    expect(counters.batches).toBe(batchesAfterConnect);

    unsubscribe();
  });

  it('recovers from transient 401s on the ticket exchange without a restart (the incident test)', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    const { c, fetchMock } = makeClient();
    const counters = { tickets: 0, batches: 0 };
    // The exact LaunchDarkly 2026-07-10 failure shape: the control plane
    // rejects valid credentials transiently. Two 401s, then recovery.
    routeFetch(
      fetchMock,
      () => (counters.tickets <= 2 ? { ok: false, status: 401 } : { ok: true }),
      counters,
    );

    const onChange = vi.fn();
    const states: StreamConnectionState[] = [];
    const unsubscribe = c.stream({}, onChange, {
      pollingIntervalMs: 10_000,
      onStateChange: (s) => states.push(s),
    });

    // 1st attempt 401s immediately → reconnecting, polling keeps data fresh.
    await vi.advanceTimersByTimeAsync(0);
    expect(counters.tickets).toBe(1);
    expect(states[states.length - 1]).toBe('reconnecting');
    expect(onChange).toHaveBeenCalled();

    // 2nd attempt after 500ms (r=0.5·1000) also 401s.
    await vi.advanceTimersByTimeAsync(500);
    expect(counters.tickets).toBe(2);

    // 3rd attempt after another 1000ms (r=0.5·2000) succeeds → streaming.
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.advanceTimersByTimeAsync(0);
    expect(counters.tickets).toBe(3);
    expect(FakeEventSource.instances.length).toBe(1);
    FakeEventSource.instances[0]!.emit('connected', '{}');
    await vi.advanceTimersByTimeAsync(0);
    expect(states[states.length - 1]).toBe('streaming');

    // Both 401s happened within the dedup window → exactly one warning.
    expect(console.warn).toHaveBeenCalledTimes(1);

    unsubscribe();
  });

  it('treats a 402 feature gate as polling-only — no SSE retries against a closed door', async () => {
    const { c, fetchMock } = makeClient();
    const counters = { tickets: 0, batches: 0 };
    routeFetch(fetchMock, () => ({ ok: false, status: 402 }), counters);

    const states: StreamConnectionState[] = [];
    const unsubscribe = c.stream({}, () => void 0, {
      pollingIntervalMs: 10_000,
      onStateChange: (s) => states.push(s),
    });

    await vi.advanceTimersByTimeAsync(0);
    expect(states[states.length - 1]).toBe('polling');

    // Five minutes later: exactly one ticket attempt ever, no EventSource,
    // and polling has kept refreshing.
    await vi.advanceTimersByTimeAsync(300_000);
    expect(counters.tickets).toBe(1);
    expect(FakeEventSource.instances.length).toBe(0);
    expect(counters.batches).toBeGreaterThanOrEqual(30);

    unsubscribe();
  });

  it('caps full-jitter backoff at maxDelayMs', async () => {
    // r=1 makes each delay equal its ceiling: 1s, 2s, 4s, then capped at 4s.
    vi.spyOn(Math, 'random').mockReturnValue(1);
    const { c, fetchMock } = makeClient({ baseDelayMs: 1_000, maxDelayMs: 4_000 });
    const counters = { tickets: 0, batches: 0 };
    routeFetch(fetchMock, () => ({ ok: false, status: 500 }), counters);

    const unsubscribe = c.stream({}, () => void 0, { pollingIntervalMs: 60_000 });

    await vi.advanceTimersByTimeAsync(0);
    expect(counters.tickets).toBe(1);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(counters.tickets).toBe(2);
    await vi.advanceTimersByTimeAsync(2_000);
    expect(counters.tickets).toBe(3);
    await vi.advanceTimersByTimeAsync(4_000);
    expect(counters.tickets).toBe(4);
    // Ceiling is now pinned at maxDelayMs — 4s, not 8s.
    await vi.advanceTimersByTimeAsync(4_000);
    expect(counters.tickets).toBe(5);

    unsubscribe();
  });

  it('resets the backoff ladder after a connection stays healthy for 30s', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(1);
    const { c, fetchMock } = makeClient();
    const counters = { tickets: 0, batches: 0 };
    // Two failures ratchet the ladder up, then a success.
    routeFetch(
      fetchMock,
      () => (counters.tickets <= 2 ? { ok: false, status: 500 } : { ok: true }),
      counters,
    );

    const unsubscribe = c.stream({}, () => void 0, { pollingIntervalMs: 60_000 });

    await vi.advanceTimersByTimeAsync(0); // failure 1 (attempt → 1)
    await vi.advanceTimersByTimeAsync(1_000); // failure 2 (attempt → 2)
    await vi.advanceTimersByTimeAsync(2_000); // success → EventSource opens
    await vi.advanceTimersByTimeAsync(0);
    expect(FakeEventSource.instances.length).toBe(1);
    const es = FakeEventSource.instances[0]!;
    es.emit('connected', '{}');
    await vi.advanceTimersByTimeAsync(0);

    // Healthy for 30s → ladder resets. Keep the watchdog quiet with a
    // heartbeat, then kill the connection.
    await vi.advanceTimersByTimeAsync(30_000);
    es.emit('heartbeat', '{}');
    es.onerror?.(new Event('error'));
    await vi.advanceTimersByTimeAsync(0);

    // Reset ladder → next delay is base·2^0 = 1s (r=1), not 4s.
    const ticketsBefore = counters.tickets;
    await vi.advanceTimersByTimeAsync(1_000);
    expect(counters.tickets).toBe(ticketsBefore + 1);

    unsubscribe();
  });

  it('watchdog reconnects a silently dead stream; heartbeats keep it alive', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(1);
    const { c, fetchMock } = makeClient();
    const counters = { tickets: 0, batches: 0 };
    routeFetch(fetchMock, () => ({ ok: true }), counters);

    const unsubscribe = c.stream({}, () => void 0, { pollingIntervalMs: 60_000 });
    await vi.advanceTimersByTimeAsync(0);
    const es1 = FakeEventSource.instances[0]!;
    es1.emit('connected', '{}');
    await vi.advanceTimersByTimeAsync(0);

    // Heartbeats every 30s keep the watchdog satisfied.
    for (let i = 0; i < 3; i++) {
      await vi.advanceTimersByTimeAsync(30_000);
      es1.emit('heartbeat', '{}');
    }
    expect(FakeEventSource.instances.length).toBe(1);
    expect(es1.closed).toBe(false);

    // Then the connection goes silent — no error event, just nothing. The
    // watchdog (checks every 30s) trips once silence exceeds 90s.
    await vi.advanceTimersByTimeAsync(120_000);
    expect(es1.closed).toBe(true);
    // And a replacement connection is already being established.
    await vi.advanceTimersByTimeAsync(1_000);
    await vi.advanceTimersByTimeAsync(0);
    expect(FakeEventSource.instances.length).toBe(2);

    unsubscribe();
  });

  it('retry.enabled:false restores the legacy downgrade-to-polling-forever behavior', async () => {
    const { c, fetchMock } = makeClient({ enabled: false });
    const counters = { tickets: 0, batches: 0 };
    routeFetch(fetchMock, () => ({ ok: true }), counters);

    const states: StreamConnectionState[] = [];
    const unsubscribe = c.stream({}, () => void 0, {
      pollingIntervalMs: 10_000,
      onStateChange: (s) => states.push(s),
    });

    await vi.advanceTimersByTimeAsync(0);
    const es1 = FakeEventSource.instances[0]!;
    es1.emit('connected', '{}');
    await vi.advanceTimersByTimeAsync(0);

    es1.onerror?.(new Event('error'));
    await vi.advanceTimersByTimeAsync(0);
    expect(states[states.length - 1]).toBe('polling');

    // Ten minutes later: still exactly one EventSource ever, one ticket, and
    // polling is doing the work.
    await vi.advanceTimersByTimeAsync(600_000);
    expect(FakeEventSource.instances.length).toBe(1);
    expect(counters.tickets).toBe(1);
    expect(counters.batches).toBeGreaterThan(10);

    unsubscribe();
  });
});
