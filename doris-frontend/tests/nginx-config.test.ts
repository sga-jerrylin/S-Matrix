import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const template = readFileSync(new URL('../nginx.conf.template', import.meta.url), 'utf8');

test('nginx api proxy uses extended timeouts for long-running natural queries', () => {
  assert.match(template, /proxy_connect_timeout\s+30s;/);
  assert.match(template, /proxy_read_timeout\s+300s;/);
  assert.match(template, /proxy_send_timeout\s+300s;/);
  assert.match(template, /send_timeout\s+300s;/);
});
