import { describe, it, expect, vi, beforeEach } from 'vitest';
import { ShipSilentlyClient } from './index';

function okEvaluate(url: string): Response {
  if (url.endsWith('/v1/evaluate')) {
    return {
      ok: true,
      status: 200,
      json: async () => ({ flagKey: 'a', value: true, reason: 'default' }),
    } as unknown as Response;
  }
  if (url.endsWith('/v1/evaluate/batch')) {
    return {
      ok: true,
      status: 200,
      json: async () => ({
        flags: { a: { flagKey: 'a', value: true, reason: 'default' } },
      }),
    } as unknown as Response;
  }
  if (url.endsWith('/v1/analytics/events')) {
    return { ok: true, status: 200, json: async () => ({ accepted: 1 }) } as unknown as Response;
  }
  return { ok: false, status: 500 } as Response;
}

describe('ShipSilently SDK — exactly-once counting semantics', () => {
  it('does NOT buffer a client event for a successful evaluate() (server already counts it)', async () => {
    const fetchMock = vi.fn().mockImplementation(async (url: string) => okEvaluate(String(url)));
    const client = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost',
      fetch: fetchMock as typeof globalThis.fetch,
      analytics: { flushIntervalMs: 60_000, maxBatchSize: 100 },
    });

    const value = await client.evaluate('a', {}, false);
    expect(value).toBe(true);
    // The server records this evaluation as source=server. A client event too
    // would make one flag check count twice on the dashboard.
    expect(client._bufferSize()).toBe(0);
  });

  it('does NOT buffer client events for a successful evaluateAll() hydration', async () => {
    const fetchMock = vi.fn().mockImplementation(async (url: string) => okEvaluate(String(url)));
    const client = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost',
      fetch: fetchMock as typeof globalThis.fetch,
      analytics: { flushIntervalMs: 60_000, maxBatchSize: 100 },
    });

    const flags = await client.evaluateAll({ userId: 'u' });
    expect(Object.keys(flags)).toEqual(['a']);
    expect(client._bufferSize()).toBe(0);
  });

  it('buffers a client event for every get() cache read', async () => {
    const fetchMock = vi.fn().mockImplementation(async (url: string) => okEvaluate(String(url)));
    const client = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost',
      fetch: fetchMock as typeof globalThis.fetch,
      analytics: { flushIntervalMs: 60_000, maxBatchSize: 100 },
    });

    await client.evaluateAll({ userId: 'u' });
    client.get('a', false);
    client.get('a', false);
    client.get('missing', false);
    expect(client._bufferSize()).toBe(3);

    const flushed = await client.flushAnalytics();
    expect(flushed).toBe(3);
    expect(client._bufferSize()).toBe(0);

    const flushCall = fetchMock.mock.calls.find((args) =>
      String(args[0]).includes('/v1/analytics/events'),
    );
    expect(flushCall).toBeDefined();
    const body = JSON.parse((flushCall![1] as RequestInit).body as string);
    expect(body.events).toHaveLength(3);
    expect(body.events[0].flagKey).toBe('a');
    expect(body.events[0].variationKey).toBe('true');
    expect(body.events[0].reason).toBe('default');
    // Cache miss records the locally-served default.
    expect(body.events[2].flagKey).toBe('missing');
    expect(body.events[2].reason).toBe('flag_not_found');
  });

  it('records a locally-served fallback when evaluate() fails (server saw nothing)', async () => {
    const fetchMock = vi.fn().mockImplementation(async (url: string) => {
      if (String(url).endsWith('/v1/evaluate')) {
        return { ok: false, status: 500, json: async () => ({}) } as unknown as Response;
      }
      return okEvaluate(String(url));
    });
    const client = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost',
      fetch: fetchMock as typeof globalThis.fetch,
      analytics: { flushIntervalMs: 60_000, maxBatchSize: 100 },
    });

    const value = await client.evaluate('a', {}, false);
    expect(value).toBe(false);
    expect(client._bufferSize()).toBe(1);

    const flushFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ accepted: 1 }),
    } as unknown as Response);
    (client as unknown as { fetch: typeof globalThis.fetch }).fetch =
      flushFetch as unknown as typeof globalThis.fetch;
    await client.flushAnalytics();
    const body = JSON.parse((flushFetch.mock.calls[0]![1] as RequestInit).body as string);
    expect(body.events[0].reason).toBe('error_fallback');
    expect(body.events[0].variationKey).toBe('false');
  });

  it('records flag_not_found (not error_fallback) when evaluate() gets a 404', async () => {
    const fetchMock = vi.fn().mockImplementation(async (url: string) => {
      if (String(url).endsWith('/v1/evaluate')) {
        return { ok: false, status: 404, json: async () => ({}) } as unknown as Response;
      }
      return okEvaluate(String(url));
    });
    const client = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost',
      fetch: fetchMock as typeof globalThis.fetch,
      analytics: { flushIntervalMs: 60_000, maxBatchSize: 100 },
    });

    await client.evaluate('ghost', {}, false);
    const flushFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ accepted: 1 }),
    } as unknown as Response);
    (client as unknown as { fetch: typeof globalThis.fetch }).fetch =
      flushFetch as unknown as typeof globalThis.fetch;
    await client.flushAnalytics();
    const body = JSON.parse((flushFetch.mock.calls[0]![1] as RequestInit).body as string);
    expect(body.events[0].reason).toBe('flag_not_found');
  });
});

describe('ShipSilently SDK — analytics buffering & flushing', () => {
  it('auto-flushes once the buffer hits maxBatchSize', async () => {
    const flushSpy = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ accepted: 2 }),
    } as unknown as Response);
    const fetchMock = vi.fn().mockImplementation(async (url: string) => {
      if (String(url).endsWith('/v1/analytics/events')) return flushSpy(url);
      return okEvaluate(String(url));
    });

    const client = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost',
      fetch: fetchMock as typeof globalThis.fetch,
      analytics: { flushIntervalMs: 60_000, maxBatchSize: 2 },
    });

    await client.evaluateAll({ userId: 'u' });
    client.get('a', false);
    client.get('a', false);

    // Wait a microtask for the auto-flush to fire.
    await new Promise((r) => setTimeout(r, 5));
    expect(flushSpy).toHaveBeenCalled();
  });

  it('honors enabled=false by skipping all buffering', async () => {
    const fetchMock = vi.fn().mockImplementation(async (url: string) => okEvaluate(String(url)));
    const client = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost',
      fetch: fetchMock as typeof globalThis.fetch,
      analytics: { enabled: false },
    });

    await client.evaluateAll({});
    client.get('a', false);
    expect(client._bufferSize()).toBe(0);
    const flushed = await client.flushAnalytics();
    expect(flushed).toBe(0);
  });

  it('drops oldest events when the buffer exceeds maxBufferSize', async () => {
    const fetchMock = vi.fn().mockImplementation(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ flagKey: 'a', value: true, reason: 'default' }),
    } as unknown as Response));

    const client = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost',
      fetch: fetchMock as typeof globalThis.fetch,
      analytics: {
        flushIntervalMs: 60_000,
        maxBatchSize: 1_000,
        maxBufferSize: 3,
      },
    });

    // Drive the internal recorder directly so we can verify the bound.
    for (let i = 0; i < 10; i++) {
      client.recordEvaluation({
        flagKey: 'flag',
        variationKey: 'true',
        reason: 'default',
        latencyMs: 1,
        timestamp: 0,
      });
    }
    expect(client._bufferSize()).toBeLessThanOrEqual(3);
  });

  it('records variation keys consistently across cached reads', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        flags: {
          'priming': { flagKey: 'priming', value: 'green', reason: 'rule_match' },
        },
      }),
    } as unknown as Response);

    const client = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost',
      fetch: fetchMock as typeof globalThis.fetch,
      analytics: { flushIntervalMs: 60_000, maxBatchSize: 100 },
    });

    await client.evaluateAll({ userId: 'u' });
    client.get('priming', '');
    client.get('priming', '');

    expect(client._bufferSize()).toBe(2);
    // Capture the buffer contents by flushing and inspecting the request body.
    const flushFetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ accepted: 1 }),
    } as unknown as Response);
    (client as unknown as { fetch: typeof globalThis.fetch }).fetch = flushFetch as unknown as typeof globalThis.fetch;

    await client.flushAnalytics();
    const body = JSON.parse((flushFetch.mock.calls[0]![1] as RequestInit).body as string);
    const variations = body.events.map((e: { variationKey: string }) => e.variationKey);
    // Every variation key must be a JSON-encoded "green" string.
    expect(variations.every((v: string) => v === '"green"')).toBe(true);
  });

  it('re-buffers a throttled (429) flush so metrics are not lost', async () => {
    // A rate-limited flush must NOT drop events — otherwise a busy client
    // silently loses metrics and the dashboard stays empty.
    const fetchMock = vi.fn().mockImplementation(async (url: string) => {
      if (String(url).endsWith('/v1/analytics/events')) {
        return {
          ok: false,
          status: 429,
          json: async () => ({ error: 'rate_limit_exceeded' }),
        } as unknown as Response;
      }
      return okEvaluate(String(url));
    });

    const client = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost',
      fetch: fetchMock as typeof globalThis.fetch,
      analytics: { flushIntervalMs: 60_000, maxBatchSize: 100 },
    });

    await client.evaluateAll({});
    client.get('a', false);
    expect(client._bufferSize()).toBe(1);

    const flushed = await client.flushAnalytics();
    expect(flushed).toBe(0);
    // Events stay buffered for the next flush instead of being dropped.
    expect(client._bufferSize()).toBe(1);
  });

  it('drops events on a hard 4xx (400) — replay cannot succeed', async () => {
    const fetchMock = vi.fn().mockImplementation(async (url: string) => {
      if (String(url).endsWith('/v1/analytics/events')) {
        return {
          ok: false,
          status: 400,
          json: async () => ({ error: 'invalid_payload' }),
        } as unknown as Response;
      }
      return okEvaluate(String(url));
    });

    const client = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost',
      fetch: fetchMock as typeof globalThis.fetch,
      analytics: { flushIntervalMs: 60_000, maxBatchSize: 100 },
    });

    await client.evaluateAll({});
    client.get('a', false);
    expect(client._bufferSize()).toBe(1);

    const flushed = await client.flushAnalytics();
    expect(flushed).toBe(0);
    // A malformed batch is dropped so it can't grow the buffer unbounded.
    expect(client._bufferSize()).toBe(0);
  });

  beforeEach(() => {
    vi.restoreAllMocks();
  });
});

describe('ShipSilently SDK — flush chunking (server caps batches at 1000 events)', () => {
  function bufferN(client: ShipSilentlyClient, n: number): void {
    for (let i = 0; i < n; i++) {
      client.recordEvaluation({
        flagKey: `flag-${i % 7}`,
        variationKey: 'true',
        reason: 'default',
        latencyMs: 1,
        timestamp: 0,
      });
    }
  }

  it('splits an oversized buffer into sequential requests of at most 1000 events', async () => {
    const batchSizes: number[] = [];
    const fetchMock = vi.fn().mockImplementation(async (_url: string, init?: RequestInit) => {
      const body = JSON.parse(init?.body as string) as { events: unknown[] };
      batchSizes.push(body.events.length);
      return { ok: true, status: 200, json: async () => ({ accepted: body.events.length }) } as unknown as Response;
    });

    const client = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost',
      fetch: fetchMock as typeof globalThis.fetch,
      // maxBatchSize is the auto-flush trigger, not the request size cap; use
      // a huge value so recordEvaluation doesn't auto-flush mid-setup.
      analytics: { flushIntervalMs: 60_000, maxBatchSize: 100_000, maxBufferSize: 100_000 },
    });

    bufferN(client, 2_500);
    const flushed = await client.flushAnalytics();

    expect(flushed).toBe(2_500);
    expect(batchSizes).toEqual([1_000, 1_000, 500]);
    expect(client._bufferSize()).toBe(0);
  });

  it('re-buffers the failed chunk AND the unsent remainder on a retryable failure', async () => {
    let calls = 0;
    const fetchMock = vi.fn().mockImplementation(async () => {
      calls += 1;
      if (calls === 1) {
        return { ok: true, status: 200, json: async () => ({ accepted: 1000 }) } as unknown as Response;
      }
      return { ok: false, status: 503, json: async () => ({}) } as unknown as Response;
    });

    const client = new ShipSilentlyClient({
      apiKey: 'k',
      apiUrl: 'http://localhost',
      fetch: fetchMock as typeof globalThis.fetch,
      analytics: { flushIntervalMs: 60_000, maxBatchSize: 100_000, maxBufferSize: 100_000 },
    });

    bufferN(client, 2_500);
    const flushed = await client.flushAnalytics();

    // First chunk of 1000 landed; the failing second chunk and the untried
    // remainder (1500 total) are preserved for the next flush.
    expect(flushed).toBe(1_000);
    expect(client._bufferSize()).toBe(1_500);
  });
});
