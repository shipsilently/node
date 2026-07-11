# @shipsilently/node

Official Node.js / universal JavaScript & TypeScript SDK for
[ShipSilently](https://shipsilently.com) — Cloudflare-native feature flags.

> **This repository is an automated, read-only mirror.** Source lives in the
> ShipSilently monorepo and is synced here on every change. Please do not open
> pull requests against this repo — they will be overwritten on the next sync.
> For issues or contributions, contact `hello@shipsilently.com`.

Ships first-class TypeScript types and works with both ESM (`import`) and
CommonJS (`require`) — no configuration required.

Building a React app? See [`@shipsilently/react`](https://github.com/shipsilently/react)
for a hooks/provider wrapper around this SDK.

## Install

```bash
npm install @shipsilently/node
# or
bun add @shipsilently/node
```

## Usage

```ts
// ESM / TypeScript
import { ShipSilentlyClient } from '@shipsilently/node';
```

```js
// CommonJS
const { ShipSilentlyClient } = require('@shipsilently/node');
```

```ts
import { ShipSilentlyClient } from '@shipsilently/node';

const client = new ShipSilentlyClient({
  apiKey: process.env.SHIPSILENTLY_API_KEY!,
});

// Evaluate a single flag with a default fallback.
const checkoutV2 = await client.evaluate('checkout-v2', { userId: 'user_123' }, false);

if (checkoutV2) {
  // ...new checkout flow
}

// Evaluate every flag for a user context at once.
const all = await client.evaluateAll({ userId: 'user_123', plan: 'pro' });
```

See the [ShipSilently docs](https://shipsilently.com) for the full API.

## License

[MIT](./LICENSE) © ShipSilently
