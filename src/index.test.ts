import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { ShipSilentlyClient } from '../src/index';

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

  it('returns defaultValue when API returns non-ok status', async () => {
    mockFetch.mockResolvedValue({ ok: false, status: 401 } as Response);
    const val = await client.evaluate('my-flag', { userId: 'u1' }, 'fallback');
    expect(val).toBe('fallback');
  });

  it('returns defaultValue on fetch failure (non-ok 500)', async () => {
    mockFetch.mockResolvedValue({ ok: false, status: 500 } as Response);
    const val = await client.evaluate('my-flag', {}, 42);
    expect(val).toBe(42);
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

  it('returns empty object on non-ok response', async () => {
    mockFetch.mockResolvedValue({ ok: false, status: 500 } as Response);
    const flags = await client.evaluateAll({});
    expect(flags).toEqual({});
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
