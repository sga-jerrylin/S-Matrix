<template>
  <div class="natural-query">
    <a-card title="自然语言查询 (AI Agent)">
      <a-alert
        message="使用自然语言提问,AI 会自动生成 SQL 并执行查询"
        type="info"
        show-icon
        style="margin-bottom: 16px"
      />

      <a-form :model="queryForm" layout="vertical">
        <a-form-item label="AI 配置" required>
          <a-row :gutter="16">
            <a-col :span="12">
              <a-select
                v-model:value="queryForm.selectedResource"
                placeholder="选择已配置的 AI 资源"
                :loading="resourcesLoading"
                @focus="loadResources"
                allow-clear
              >
                <a-select-option v-for="resource in resources" :key="resource.name" :value="resource.name">
                  {{ resource.name }} ({{ resource.provider }})
                </a-select-option>
              </a-select>
            </a-col>
            <a-col :span="12">
              <a-input
                v-model:value="queryForm.apiKey"
                placeholder="或手动输入 API Key"
                type="password"
              />
            </a-col>
          </a-row>
        </a-form-item>

        <a-form-item label="查询范围">
          <a-select
            v-model:value="queryForm.selectedTables"
            mode="multiple"
            allow-clear
            show-search
            option-filter-prop="label"
            placeholder="默认自动选择；可指定 1 张或多张表来缩小 AI 查询范围"
            :loading="tablesLoading"
            :options="tableOptions"
            :max-tag-count="responsiveTagCount"
            @focus="loadTables"
          />
          <div class="scope-hint">
            当前系统只有一个 Doris 库；这里按已同步表限制 AI 的查询范围，能减少全表扫描式推理。
          </div>
        </a-form-item>

        <a-form-item label="自然语言问题" required>
          <a-textarea
            v-model:value="queryForm.query"
            placeholder="例如: 2022年的机构中来自于广东的有多少个?分别是来自于广东那几个城市每个城市的占比是多少?"
            :rows="4"
            :maxlength="500"
            show-count
          />
        </a-form-item>

        <a-form-item>
          <a-space>
            <a-button type="primary" @click="handleQuery" :loading="querying">
              <template #icon><SearchOutlined /></template>
              执行查询
            </a-button>
            <a-button @click="handleClear">清空</a-button>
          </a-space>
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

        <a-collapse style="margin-bottom: 16px" :default-active-key="[]">
          <a-collapse-panel key="1" header="调试信息：生成的 SQL 查询">
            <pre class="sql-code">{{ result.sql }}</pre>
          </a-collapse-panel>
        </a-collapse>

        <a-table
          :columns="resultColumns"
          :data-source="result.data"
          :pagination="{ pageSize: 20 }"
          size="small"
          :scroll="{ x: 'max-content' }"
        />
      </div>
    </a-card>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue';
import { message } from 'ant-design-vue';
import { SearchOutlined } from '@ant-design/icons-vue';
import { dorisApi } from '../api/doris';
import { extractApiErrorMessage } from '../api/errors';
import { normalizeLLMResource } from '../api/llm-resources';
import { buildNaturalQueryPayload } from './natural-query-state';

const queryForm = ref({
  query: '',
  selectedResource: undefined as string | undefined,
  apiKey: '',
  selectedTables: [] as string[],
});

const resources = ref<any[]>([]);
const resourcesLoading = ref(false);
const tablesLoading = ref(false);
const availableTables = ref<any[]>([]);
const querying = ref(false);
const result = ref<any>(null);
const responsiveTagCount = 'responsive';

const tableOptions = computed(() =>
  availableTables.value.map((table) => ({
    label: table.display_name
      ? `${table.display_name} (${table.table_name})`
      : table.table_name,
    value: table.table_name,
  })),
);

const resultColumns = computed(() => {
  if (!result.value || !result.value.data || result.value.data.length === 0) return [];
  const firstRow = result.value.data[0];
  return Object.keys(firstRow).map((key) => ({
    title: key,
    dataIndex: key,
    key: key,
    ellipsis: true,
  }));
});

const loadResources = async () => {
  if (resources.value.length > 0) return;
  
  resourcesLoading.value = true;
  try {
    const response = await dorisApi.llm.list();
    resources.value = (response.data.resources || []).map((resource: any) => normalizeLLMResource(resource));
  } catch (error: any) {
    message.error('加载 AI 资源失败: ' + extractApiErrorMessage(error));
  } finally {
    resourcesLoading.value = false;
  }
};

const loadTables = async () => {
  if (availableTables.value.length > 0) return;

  tablesLoading.value = true;
  try {
    const response = await dorisApi.tableRegistry.list();
    availableTables.value = response.data.tables || [];
  } catch (error: any) {
    message.error('加载查询范围失败: ' + extractApiErrorMessage(error));
  } finally {
    tablesLoading.value = false;
  }
};

const handleQuery = async () => {
  if (!queryForm.value.query) {
    message.warning('请输入自然语言问题');
    return;
  }

  if (!queryForm.value.selectedResource && !queryForm.value.apiKey) {
    message.warning('请选择 AI 资源或输入 API Key');
    return;
  }

  querying.value = true;
  try {
    const response = await dorisApi.naturalQuery(buildNaturalQueryPayload(queryForm.value));
    
    result.value = response.data;
    message.success('查询成功');
  } catch (error: any) {
    const errorMsg = extractApiErrorMessage(error);
    message.error('查询失败: ' + errorMsg);
    console.error('Natural query error:', error);
  } finally {
    querying.value = false;
  }
};

const handleClear = () => {
  queryForm.value.query = '';
  queryForm.value.apiKey = '';
  result.value = null;
};

onMounted(() => {
  loadResources();
  loadTables();
});
</script>

<style scoped>
.natural-query {
  padding: 24px;
}

.sql-code {
  background: #f5f5f5;
  padding: 12px;
  border-radius: 4px;
  overflow-x: auto;
  font-family: 'Courier New', monospace;
  font-size: 13px;
  line-height: 1.5;
}

.scope-hint {
  margin-top: 8px;
  color: #666;
  font-size: 12px;
}
</style>
