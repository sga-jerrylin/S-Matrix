import test from 'node:test';
import assert from 'node:assert/strict';

import { normalizeLLMResource } from '../src/api/llm-resources.ts';

test('normalizeLLMResource supports Doris SHOW RESOURCES payload', () => {
  const normalized = normalizeLLMResource({
    ResourceName: 'Deepseek',
    ResourceType: 'ai',
    properties: {
      'ai.provider_type': 'DEEPSEEK',
      'ai.model_name': 'deepseek-chat',
      'ai.endpoint': 'https://api.deepseek.com/chat/completions',
      'ai.api_key': '******',
      'ai.temperature': '0.2',
      'ai.max_tokens': '1024',
    },
    last_test_status: 'failed',
    last_test_error: '401 unauthorized',
  });

  assert.equal(normalized.name, 'Deepseek');
  assert.equal(normalized.provider, 'DEEPSEEK');
  assert.equal(normalized.model, 'deepseek-chat');
  assert.equal(normalized.endpoint, 'https://api.deepseek.com/chat/completions');
  assert.equal(normalized.apiKeyConfigured, true);
  assert.equal(normalized.temperature, 0.2);
  assert.equal(normalized.maxTokens, 1024);
  assert.equal(normalized.lastTestStatus, 'failed');
  assert.equal(normalized.lastTestError, '401 unauthorized');
});

test('normalizeLLMResource treats Doris sentinel values as empty', () => {
  const normalized = normalizeLLMResource({
    ResourceName: 'ds',
    ResourceType: 'ai',
    properties: {
      'ai.provider_type': 'DEEPSEEK',
      'ai.model_name': 'deepseek-v4-pro',
      'ai.endpoint': 'https://api.deepseek.com/chat/completions',
      'ai.temperature': '-1',
      'ai.max_tokens': '-1',
    },
  });

  assert.equal(normalized.temperature, undefined);
  assert.equal(normalized.maxTokens, undefined);
});
