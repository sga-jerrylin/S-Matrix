import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

import {
  buildDataQueryPayload,
  buildFieldOptions,
  buildRelationshipKey,
  buildTableLabel,
} from '../src/components/data-query-state.ts';

const institutions = {
  table_name: 'institutions',
  display_name: '机构基础表',
  description: '机构主表',
  fields: [
    {
      field_name: '机构名称',
      display_name: '机构名称',
      description: '机构正式名称',
      field_type: 'VARCHAR',
    },
    {
      field_name: '所在市',
      display_name: '城市',
      description: '所在城市',
      field_type: 'VARCHAR',
    },
  ],
  relationships: [
    {
      related_table_name: 'activities',
      related_display_name: '活动参与表',
      relation_label: '活动参与表 · 通过“机构ID = 机构ID”关联到“活动参与表”',
      relation_description: '通过“机构ID = 机构ID”关联到“活动参与表”',
      source_field_name: 'institution_id',
      source_field_display_name: '机构ID',
      target_field_name: 'institution_id',
      target_field_display_name: '机构ID',
    },
  ],
};

const activities = {
  table_name: 'activities',
  display_name: '活动参与表',
  description: '活动明细',
  fields: [
    {
      field_name: '活动名称',
      display_name: '活动名称',
      description: '活动标题',
      field_type: 'VARCHAR',
    },
  ],
  relationships: [],
};

test('buildTableLabel prefers display name for business tables', () => {
  assert.equal(buildTableLabel(institutions), '机构基础表 (institutions)');
});

test('buildFieldOptions includes related table fields when a relationship is selected', () => {
  const relationship = institutions.relationships[0];
  const options = buildFieldOptions(institutions, relationship, {
    institutions,
    activities,
  });

  assert.equal(options.length, 3);
  assert.equal(options[0].label, '机构基础表 · 机构名称');
  assert.equal(options[1].label, '机构基础表 · 城市 (所在市)');
  assert.equal(options[2].label, '活动参与表 · 活动名称');
});

test('buildFieldOptions keeps single-table labels compact for analysis fields', () => {
  const options = buildFieldOptions(
    {
      ...institutions,
      fields: [
        {
          field_name: '枢纽组织',
          display_name: '是否为枢纽型组织',
          description: '是否为枢纽型组织，数据类型：字符串（可能为空）',
          field_type: 'VARCHAR',
        },
      ],
    },
    undefined,
    { institutions, activities },
  );

  assert.equal(options.length, 1);
  assert.equal(options[0].label, '是否为枢纽型组织');
});

test('buildDataQueryPayload sends semantic join metadata for query action', () => {
  const relationship = institutions.relationships[0];
  const options = buildFieldOptions(institutions, relationship, {
    institutions,
    activities,
  });

  const payload = buildDataQueryPayload({
    form: {
      action: 'query',
      table: 'institutions',
      relationshipKey: buildRelationshipKey(relationship),
      selectedColumns: [options[0].value, options[2].value],
      selectedColumn: undefined,
    },
    params: { limit: 50 },
    fieldOptions: options,
    relationship,
  });

  assert.deepEqual(payload, {
    action: 'query',
    table: 'institutions',
    params: {
      limit: 50,
      selected_fields: [
        {
          table_name: 'institutions',
          field_name: '机构名称',
          label: '机构基础表 机构名称',
        },
        {
          table_name: 'activities',
          field_name: '活动名称',
          label: '活动参与表 活动名称',
        },
      ],
      join_table: 'activities',
      join_left_column: 'institution_id',
      join_right_column: 'institution_id',
    },
  });
});

test('data query component defaults to business tables and relationship query semantics', () => {
  const component = readFileSync(new URL('../src/components/DataQuery.vue', import.meta.url), 'utf8');

  assert.match(component, /默认只显示已注册业务表/);
  assert.match(component, /label="业务表"/);
  assert.match(component, /label="关联查询"/);
  assert.match(component, /:default-active-key="\[\]"/);
});
