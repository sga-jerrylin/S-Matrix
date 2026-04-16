import test from 'node:test';
import assert from 'node:assert/strict';

import {
  applyGatewayAuthHeaders,
  resolveGatewayApiKey,
  STORAGE_KEY,
} from '../src/api/auth.ts';

class MemoryStorage {
  private values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.has(key) ? this.values.get(key)! : null;
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }

  removeItem(key: string): void {
    this.values.delete(key);
  }
}

test('resolveGatewayApiKey uses env fallback when storage is empty', () => {
  const storage = new MemoryStorage();

  const apiKey = resolveGatewayApiKey({ envApiKey: 'env-key', storage });

  assert.equal(apiKey, 'env-key');
});

test('resolveGatewayApiKey prefers trimmed storage value over env', () => {
  const storage = new MemoryStorage();
  storage.setItem(STORAGE_KEY, '  browser-key  ');

  const apiKey = resolveGatewayApiKey({ envApiKey: 'env-key', storage });

  assert.equal(apiKey, 'browser-key');
});

test('applyGatewayAuthHeaders injects both auth headers', () => {
  const headers = applyGatewayAuthHeaders({ 'Content-Type': 'application/json' }, 'local-key');

  assert.equal(headers['X-API-Key'], 'local-key');
  assert.equal(headers.Authorization, 'Bearer local-key');
  assert.equal(headers['Content-Type'], 'application/json');
});
