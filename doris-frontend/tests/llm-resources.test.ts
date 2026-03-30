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
    },
  });

  assert.equal(normalized.name, 'Deepseek');
  assert.equal(normalized.provider, 'DEEPSEEK');
  assert.equal(normalized.model, 'deepseek-chat');
  assert.equal(normalized.endpoint, 'https://api.deepseek.com/chat/completions');
  assert.equal(normalized.apiKeyConfigured, true);
});
