import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

test('upload workbench separates synced tables from source tables', () => {
  const workbench = readFileSync(new URL('../src/components/WorkbenchUpload.vue', import.meta.url), 'utf8');
  const datasource = readFileSync(new URL('../src/components/DataSourceSync.vue', import.meta.url), 'utf8');

  assert.match(workbench, /已同步表格 \(\$\{registryCount\}\)/);
  assert.match(workbench, /@view-registry="activeTab = 'registry'"/);
  assert.match(workbench, /@change="onTabChange"/);
  assert.match(workbench, /applyDefaultTab\(count\)/);
  assert.match(workbench, /if \(userHasSwitchedTab\.value\) return/);
  assert.match(datasource, /title="源库可同步表"/);
  assert.match(datasource, /本地已同步 \$\{localSyncedTableCount\} 张表/);
});

test('source table load failure renders explicit error state and registry shortcut', () => {
  const datasource = readFileSync(new URL('../src/components/DataSourceSync.vue', import.meta.url), 'utf8');

  assert.match(datasource, /源库表加载失败/);
  assert.match(datasource, /源库当前不可用，但本地已同步表仍可用于查询、洞察和预测/);
  assert.match(datasource, /进入已同步表格/);
  assert.match(datasource, /remoteTablesError/);
  assert.match(datasource, /tablesRes\.data\?\.success === false/);
  assert.match(datasource, /extractDatasourcePayloadError/);
  assert.match(datasource, /goToRegistry\(\)/);
});

test('new datasource form disables browser autofill and stays separate from selected source', () => {
  const datasource = readFileSync(new URL('../src/components/DataSourceSync.vue', import.meta.url), 'utf8');

  assert.match(datasource, /这是新增数据源表单，不是当前数据源编辑区/);
  assert.match(datasource, /autocomplete="off"/);
  assert.match(datasource, /name="new-datasource-db-user"/);
  assert.match(datasource, /name="new-datasource-db-secret"/);
});
