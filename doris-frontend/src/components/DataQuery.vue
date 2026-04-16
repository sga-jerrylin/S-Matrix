<template>
  <div class="data-query">
    <a-card title="数据查询与分析">
      <a-alert
        message="默认只显示已注册业务表；选择表后会同步加载字段语义和可用关联关系。"
        type="info"
        show-icon
        style="margin-bottom: 16px"
      />

      <a-form :model="queryForm" layout="vertical">
        <a-row :gutter="16">
          <a-col :span="8">
            <a-form-item label="操作类型" required>
              <a-select v-model:value="queryForm.action" placeholder="选择操作" @change="handleActionChange">
                <a-select-option value="query">普通查询</a-select-option>
                <a-select-option value="sentiment">情感分析</a-select-option>
                <a-select-option value="classify">文本分类</a-select-option>
                <a-select-option value="extract">信息提取</a-select-option>
                <a-select-option value="stats">统计分析</a-select-option>
                <a-select-option value="similarity">语义相似度</a-select-option>
                <a-select-option value="translate">文本翻译</a-select-option>
                <a-select-option value="summarize">文本摘要</a-select-option>
                <a-select-option value="mask">敏感信息脱敏</a-select-option>
                <a-select-option value="fixgrammar">语法纠错</a-select-option>
              </a-select>
            </a-form-item>
          </a-col>

          <a-col :span="8">
            <a-form-item label="业务表" required>
              <a-select
                v-model:value="queryForm.table"
                placeholder="选择已注册业务表"
                show-search
                allow-clear
                option-filter-prop="label"
                :loading="catalogLoading"
                :options="tableOptions"
                @focus="loadCatalog"
                @change="handleTableChange"
              />
            </a-form-item>
          </a-col>

          <a-col :span="8">
            <a-form-item :label="columnLabel" :required="requiresColumn">
              <a-select
                v-if="queryForm.action === 'query'"
                v-model:value="queryForm.selectedColumns"
                mode="multiple"
                allow-clear
                show-search
                option-filter-prop="label"
                placeholder="不选则默认返回整张表；选择后仅返回对应字段"
                :options="fieldOptions"
                :disabled="!selectedTable"
                :max-tag-count="'responsive'"
              />
              <a-select
                v-else
                v-model:value="queryForm.selectedColumn"
                allow-clear
                show-search
                option-filter-prop="label"
                placeholder="选择要分析的字段"
                :options="fieldOptions"
                :disabled="!selectedTable"
              />
            </a-form-item>
          </a-col>
        </a-row>

        <a-form-item v-if="queryForm.action === 'query' && selectedTable" label="关联查询">
          <a-select
            v-model:value="queryForm.relationshipKey"
            allow-clear
            show-search
            option-filter-prop="label"
            placeholder="可选：按“表显示名 + 字段显示名 + 关系说明”选择关联关系"
            :options="relationshipOptions"
            :disabled="relationshipOptions.length === 0"
            @change="handleRelationshipChange"
          />
          <div class="field-hint">
            <template v-if="selectedRelationship">
              {{ selectedRelationship.relation_description }}
            </template>
            <template v-else-if="relationshipOptions.length === 0">
              当前业务表还没有配置可用的关联关系。
            </template>
            <template v-else>
              选择后可以把关联表字段一起带入查询，不再直接暴露系统表和底层字段语义。
            </template>
          </div>
        </a-form-item>

        <a-form-item v-if="selectedTable" label="表说明">
          <a-textarea :value="selectedTable.description || '暂无业务描述'" :rows="2" readonly />
        </a-form-item>

        <a-form-item label="额外参数 (JSON)">
          <a-textarea
            v-model:value="paramsJson"
            placeholder='{"limit": 100, "filter": "base.`年份` >= 2022"}'
            :rows="3"
          />
        </a-form-item>

        <a-form-item>
          <a-button type="primary" @click="handleExecute" :loading="executing">
            执行查询
          </a-button>
        </a-form-item>
      </a-form>

      <a-divider v-if="result" />

      <div v-if="result">
        <a-alert
          :message="`查询成功 - 返回 ${result.count} 条记录`"
          type="success"
          show-icon
          style="margin-bottom: 16px"
        />

        <a-collapse v-if="result.summary" style="margin-bottom: 16px">
          <a-collapse-panel key="1" header="统计摘要">
            <a-descriptions bordered size="small" :column="2">
              <a-descriptions-item
                v-for="(value, key) in result.summary"
                :key="key"
                :label="key"
              >
                {{ value }}
              </a-descriptions-item>
            </a-descriptions>
          </a-collapse-panel>
        </a-collapse>

        <a-table
          :columns="resultColumns"
          :data-source="result.data"
          :pagination="{ pageSize: 20 }"
          size="small"
          :scroll="{ x: 'max-content' }"
        />

        <a-collapse style="margin-top: 16px" :default-active-key="[]">
          <a-collapse-panel key="1" header="调试信息：执行的 SQL">
            <pre>{{ result.sql }}</pre>
          </a-collapse-panel>
        </a-collapse>
      </div>
    </a-card>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import { message } from 'ant-design-vue';
import type { QueryCatalogRelationship, QueryCatalogTable } from '../api/doris';
import { dorisApi } from '../api/doris';
import { extractApiErrorMessage } from '../api/errors';
import {
  buildDataQueryPayload,
  buildFieldOptions,
  buildRelationshipKey,
  buildRelationshipLabel,
  buildTableLabel,
} from './data-query-state';

const queryForm = ref({
  action: 'query',
  table: '',
  relationshipKey: undefined as string | undefined,
  selectedColumns: [] as string[],
  selectedColumn: undefined as string | undefined,
});

const paramsJson = ref('');
const catalogLoading = ref(false);
const catalogTables = ref<QueryCatalogTable[]>([]);
const executing = ref(false);
const result = ref<any>(null);

const catalogByTable = computed<Record<string, QueryCatalogTable>>(() =>
  Object.fromEntries(catalogTables.value.map((table) => [table.table_name, table])),
);

const selectedTable = computed(() => catalogByTable.value[queryForm.value.table]);
const selectedRelationship = computed<QueryCatalogRelationship | undefined>(() =>
  selectedTable.value?.relationships?.find(
    (relationship) => buildRelationshipKey(relationship) === queryForm.value.relationshipKey,
  ),
);

const requiresColumn = computed(() => queryForm.value.action !== 'query');
const columnLabel = computed(() =>
  queryForm.value.action === 'query' ? '返回字段' : '分析字段',
);

const tableOptions = computed(() =>
  catalogTables.value.map((table) => ({
    label: buildTableLabel(table),
    value: table.table_name,
  })),
);

const relationshipOptions = computed(() =>
  (selectedTable.value?.relationships || []).map((relationship) => ({
    label: buildRelationshipLabel(relationship),
    value: buildRelationshipKey(relationship),
  })),
);

const fieldOptions = computed(() =>
  buildFieldOptions(selectedTable.value, selectedRelationship.value, catalogByTable.value).map((field) => ({
    label: field.label,
    value: field.value,
  })),
);

const resultColumns = computed(() => {
  if (!result.value || !result.value.data || result.value.data.length === 0) return [];
  const firstRow = result.value.data[0];
  return Object.keys(firstRow).map((key) => ({
    title: key,
    dataIndex: key,
    key,
    ellipsis: true,
  }));
});

const loadCatalog = async () => {
  if (catalogTables.value.length > 0) return;

  catalogLoading.value = true;
  try {
    const response = await dorisApi.queryCatalog.list();
    catalogTables.value = response.data.tables || [];
  } catch (error: any) {
    message.error(`加载业务表目录失败: ${extractApiErrorMessage(error)}`);
  } finally {
    catalogLoading.value = false;
  }
};

const resetFieldSelections = () => {
  queryForm.value.selectedColumns = [];
  queryForm.value.selectedColumn = undefined;
};

const handleActionChange = () => {
  resetFieldSelections();
  if (queryForm.value.action !== 'query') {
    queryForm.value.relationshipKey = undefined;
  }
};

const handleTableChange = () => {
  queryForm.value.relationshipKey = undefined;
  resetFieldSelections();
};

const handleRelationshipChange = () => {
  resetFieldSelections();
};

const parseParams = () => {
  if (!paramsJson.value) {
    return {};
  }
  try {
    return JSON.parse(paramsJson.value);
  } catch (error) {
    throw new Error('参数 JSON 格式错误');
  }
};

const handleExecute = async () => {
  if (!queryForm.value.action || !queryForm.value.table) {
    message.warning('请先选择操作类型和业务表');
    return;
  }

  if (requiresColumn.value && !queryForm.value.selectedColumn) {
    message.warning('请先选择分析字段');
    return;
  }

  let params = {};
  try {
    params = parseParams();
  } catch (error: any) {
    message.error(error.message);
    return;
  }

  executing.value = true;
  try {
    const payload = buildDataQueryPayload({
      form: queryForm.value,
      params,
      fieldOptions: buildFieldOptions(selectedTable.value, selectedRelationship.value, catalogByTable.value),
      relationship: selectedRelationship.value,
    });
    const response = await dorisApi.execute(payload);
    result.value = response.data;
    message.success('查询成功');
  } catch (error: any) {
    message.error(`查询失败: ${extractApiErrorMessage(error)}`);
  } finally {
    executing.value = false;
  }
};

onMounted(() => {
  loadCatalog();
});
</script>

<style scoped>
.data-query {
  padding: 24px;
}

.field-hint {
  margin-top: 8px;
  color: #666;
  font-size: 12px;
}

pre {
  background: #f5f5f5;
  padding: 12px;
  border-radius: 4px;
  overflow-x: auto;
}
</style>
