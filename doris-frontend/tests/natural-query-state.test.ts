import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

import { buildNaturalQueryPayload } from '../src/components/natural-query-state.ts';

test('buildNaturalQueryPayload includes selected table scope when provided', () => {
  const payload = buildNaturalQueryPayload({
    query: '2022年广东有多少机构？',
    selectedResource: 'Deepseek',
    apiKey: '',
    selectedTables: ['institutions', 'activities'],
  });

  assert.deepEqual(payload, {
    query: '2022年广东有多少机构？',
    resource_name: 'Deepseek',
    table_names: ['institutions', 'activities'],
  });
});

test('natural query SQL details are collapsed by default in the component', () => {
  const component = readFileSync(new URL('../src/components/NaturalQuery.vue', import.meta.url), 'utf8');

  assert.match(component, /label="查询范围"/);
  assert.match(component, /:default-active-key="\[\]"/);
});
