import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

test('LLM config clarifies DeepSeek endpoint contract and model lifecycle', () => {
  const component = readFileSync(new URL('../src/components/LLMConfig.vue', import.meta.url), 'utf8');

  assert.match(component, /API 端点（Doris AI_GENERATE 使用完整 chat\/completions URL）/);
  assert.match(component, /https:\/\/api\.deepseek\.com\/chat\/completions/);
  assert.match(component, /deepseek-v4-flash/);
  assert.match(component, /deepseek-v4-pro/);
  assert.match(component, /deepseek-chat.*2026-07-24.*弃用/);
  assert.match(component, /deepseek-reasoner.*2026-07-24.*弃用/);
});

test('LLM config normalizes DeepSeek base URLs before save', () => {
  const component = readFileSync(new URL('../src/components/LLMConfig.vue', import.meta.url), 'utf8');

  assert.match(component, /normalizeDeepseekEndpoint/);
  assert.match(component, /'\/v1'/);
  assert.match(component, /\/chat\/completions/);
  assert.match(component, /normalizeTemperatureForPayload/);
});
