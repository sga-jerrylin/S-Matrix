<template>
  <div class="data-query">
    <a-card title="数据查询与分析">
      <a-form :model="queryForm" layout="vertical">
        <a-row :gutter="16">
          <a-col :span="8">
            <a-form-item label="操作类型" required>
              <a-select v-model:value="queryForm.action" placeholder="选择操作">
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
            <a-form-item label="表名" required>
              <a-select
                v-model:value="queryForm.table"
                placeholder="选择表"
                show-search
                :loading="tablesLoading"
                @focus="loadTables"
              >
                <a-select-option v-for="table in tables" :key="table" :value="table">
                  {{ table }}
                </a-select-option>
              </a-select>
            </a-form-item>
          </a-col>

          <a-col :span="8">
            <a-form-item label="列名">
              <a-input v-model:value="queryForm.column" placeholder="文本列名" />
            </a-form-item>
          </a-col>
        </a-row>

        <a-form-item label="额外参数 (JSON)">
          <a-textarea
            v-model:value="paramsJson"
            placeholder='{"limit": 100, "filter": "id > 10"}'
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

        <a-collapse style="margin-top: 16px">
          <a-collapse-panel key="1" header="执行的 SQL">
            <pre>{{ result.sql }}</pre>
          </a-collapse-panel>
        </a-collapse>
      </div>
    </a-card>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue';
import { message } from 'ant-design-vue';
import { dorisApi } from '../api/doris';

const queryForm = ref({
  action: 'query',
  table: '',
  column: '',
});

const paramsJson = ref('');
const tables = ref<string[]>([]);
const tablesLoading = ref(false);
const executing = ref(false);
const result = ref<any>(null);

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

const loadTables = async () => {
  if (tables.value.length > 0) return;
  
  tablesLoading.value = true;
  try {
    const response = await dorisApi.getTables();
    tables.value = response.data.tables;
  } catch (error: any) {
    message.error('加载表列表失败: ' + (error.response?.data?.detail || error.message));
  } finally {
    tablesLoading.value = false;
  }
};

const handleExecute = async () => {
  if (!queryForm.value.action || !queryForm.value.table) {
    message.warning('请填写必填项');
    return;
  }

  let params = {};
  if (paramsJson.value) {
    try {
      params = JSON.parse(paramsJson.value);
    } catch (error) {
      message.error('参数 JSON 格式错误');
      return;
    }
  }

  executing.value = true;
  try {
    const response = await dorisApi.execute({
      action: queryForm.value.action,
      table: queryForm.value.table,
      column: queryForm.value.column || undefined,
      params,
    });
    result.value = response.data;
    message.success('查询成功');
  } catch (error: any) {
    message.error('查询失败: ' + (error.response?.data?.detail?.error || error.message));
  } finally {
    executing.value = false;
  }
};
</script>

<style scoped>
.data-query {
  padding: 24px;
}

pre {
  background: #f5f5f5;
  padding: 12px;
  border-radius: 4px;
  overflow-x: auto;
}
</style>

