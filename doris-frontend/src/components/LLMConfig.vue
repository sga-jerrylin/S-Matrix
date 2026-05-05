<template>
  <div class="llm-config">
    <a-card title="LLM 配置管理">
      <a-button type="primary" @click="openCreateModal" style="margin-bottom: 16px">
        添加 LLM 配置
      </a-button>

      <a-table
        :columns="columns"
        :data-source="resources"
        :loading="loading"
        row-key="name"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'apiKeyConfigured'">
            <a-tag :color="record.apiKeyConfigured ? 'green' : 'default'">
              {{ record.apiKeyConfigured ? '已配置' : '未配置' }}
            </a-tag>
          </template>

          <template v-else-if="column.key === 'endpoint'">
            <a-tooltip :title="record.endpoint">
              <span class="endpoint-text">{{ record.endpoint || '-' }}</span>
            </a-tooltip>
          </template>

          <template v-else-if="column.key === 'testStatus'">
            <a-space direction="vertical" size="small">
              <a-tag :color="testStatusColor(record.lastTestStatus)">
                {{ testStatusLabel(record.lastTestStatus) }}
              </a-tag>
              <span v-if="record.lastTestError" class="error-summary">
                {{ record.lastTestError }}
              </span>
            </a-space>
          </template>

          <template v-else-if="column.key === 'action'">
            <a-space>
              <a-button size="small" @click="openEditModal(record)">编辑</a-button>
              <a-popconfirm
                title="确定删除此配置?"
                @confirm="handleDelete(record.name)"
              >
                <a-button size="small" danger>删除</a-button>
              </a-popconfirm>
            </a-space>
          </template>
        </template>
      </a-table>
    </a-card>

    <a-modal
      v-model:open="showModal"
      :title="modalTitle"
      @cancel="resetForm"
      width="640px"
      :mask-closable="!submitting && !testing"
    >
      <a-alert
        v-if="modalTestMessage"
        :type="modalTestAlertType"
        :message="modalTestMessage"
        show-icon
        style="margin-bottom: 16px"
      />

      <a-form :model="formState" layout="vertical">
        <a-form-item label="资源名称" required>
          <a-input
            v-model:value="formState.resource_name"
            :disabled="isEditing"
            placeholder="例如: my_openai"
          />
        </a-form-item>

        <a-form-item label="厂商类型" required>
          <a-select v-model:value="formState.provider_type" placeholder="选择厂商" @change="handleProviderChange">
            <a-select-option value="openai">OpenAI</a-select-option>
            <a-select-option value="deepseek">DeepSeek</a-select-option>
            <a-select-option value="qwen">通义千问 (Qwen)</a-select-option>
            <a-select-option value="zhipu">智谱 (Zhipu)</a-select-option>
            <a-select-option value="moonshot">月之暗面 (MoonShot)</a-select-option>
            <a-select-option value="baichuan">百川 (Baichuan)</a-select-option>
            <a-select-option value="minimax">MiniMax</a-select-option>
            <a-select-option value="local">本地 (Ollama)</a-select-option>
          </a-select>
        </a-form-item>

        <a-form-item label="API 端点（Doris AI_GENERATE 使用完整 chat/completions URL）" required>
          <a-input v-model:value="formState.endpoint" :placeholder="endpointPlaceholder" />
          <div v-if="isDeepSeek" class="field-hint">
            输入 `https://api.deepseek.com` 或 `https://api.deepseek.com/v1` 会自动规范化为
            `https://api.deepseek.com/chat/completions`。
          </div>
        </a-form-item>

        <a-form-item label="模型名称" required>
          <a-select
            v-if="isDeepSeek"
            v-model:value="formState.model_name"
            :options="deepseekModelOptions"
            placeholder="选择 DeepSeek 模型"
          />
          <a-input v-else v-model:value="formState.model_name" placeholder="gpt-4" />
          <div v-if="isDeepSeek" class="field-hint warning-text">
            `deepseek-chat` / `deepseek-reasoner` 将于 2026-07-24 弃用，建议改为
            `deepseek-v4-flash` 或 `deepseek-v4-pro`。
          </div>
        </a-form-item>

        <a-form-item :label="isEditing ? 'API 密钥（留空表示不修改旧 key）' : 'API 密钥'">
          <a-input-password
            v-model:value="formState.api_key"
            :placeholder="isEditing ? '留空不修改旧 key' : 'sk-xxxxx (可选)'"
          />
        </a-form-item>

        <a-row :gutter="16">
          <a-col :span="12">
            <a-form-item label="Temperature (0-1)">
              <a-input-number
                v-model:value="formState.temperature"
                :min="0"
                :max="1"
                :step="0.1"
                style="width: 100%"
              />
            </a-form-item>
          </a-col>
          <a-col :span="12">
            <a-form-item label="Max Tokens">
              <a-input-number
                v-model:value="formState.max_tokens"
                :min="1"
                :max="32000"
                style="width: 100%"
              />
            </a-form-item>
          </a-col>
        </a-row>
      </a-form>

      <template #footer>
        <a-space>
          <a-button :disabled="submitting || testing" @click="resetForm">取消</a-button>
          <a-button :loading="submitting" :disabled="testing" @click="handleSubmit">
            保存
          </a-button>
          <a-button type="primary" :loading="testing" :disabled="submitting" @click="handleSaveAndTest">
            保存并测试
          </a-button>
        </a-space>
      </template>
    </a-modal>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import { message } from 'ant-design-vue';
import { dorisApi, type LLMConfigRequest } from '../api/doris';
import { normalizeLLMResource, type NormalizedLLMResource } from '../api/llm-resources';

type TestStatus = 'success' | 'failed' | 'unknown';

interface TestState {
  status: TestStatus;
  error?: string;
}

interface LLMFormState {
  resource_name: string;
  provider_type: string;
  endpoint: string;
  model_name: string;
  api_key: string;
  temperature?: number;
  max_tokens?: number;
}

const DEEPSEEK_PROVIDER = 'deepseek';
const DEEPSEEK_CANONICAL_ENDPOINT = 'https://api.deepseek.com/chat/completions';

const deepseekModelOptions = [
  { label: 'deepseek-v4-flash', value: 'deepseek-v4-flash' },
  { label: 'deepseek-v4-pro', value: 'deepseek-v4-pro' },
  { label: 'deepseek-chat（将于 2026-07-24 弃用）', value: 'deepseek-chat' },
  { label: 'deepseek-reasoner（将于 2026-07-24 弃用）', value: 'deepseek-reasoner' },
];

const columns = [
  { title: '资源名称', dataIndex: 'name', key: 'name' },
  { title: 'Provider', dataIndex: 'provider', key: 'provider' },
  { title: 'Model', dataIndex: 'model', key: 'model' },
  { title: 'Endpoint', dataIndex: 'endpoint', key: 'endpoint', ellipsis: true },
  { title: 'API Key', key: 'apiKeyConfigured', width: 110 },
  { title: '最近测试状态 / 错误摘要', key: 'testStatus', width: 220 },
  { title: '操作', key: 'action', width: 150 },
];

const resources = ref<NormalizedLLMResource[]>([]);
const testStates = ref<Record<string, TestState>>({});
const loading = ref(false);
const showModal = ref(false);
const submitting = ref(false);
const testing = ref(false);
const editingResourceName = ref<string | null>(null);
const modalTestMessage = ref('');
const modalTestAlertType = ref<'success' | 'error' | 'info' | 'warning'>('info');

const defaultFormState = (): LLMFormState => ({
  resource_name: '',
  provider_type: 'openai',
  endpoint: '',
  model_name: '',
  api_key: '',
  temperature: undefined,
  max_tokens: undefined,
});

const formState = ref<LLMFormState>(defaultFormState());

const isEditing = computed(() => Boolean(editingResourceName.value));
const modalTitle = computed(() => (isEditing.value ? '编辑 LLM 配置' : '添加 LLM 配置'));
const endpointPlaceholder = computed(() => providerEndpoints[formState.value.provider_type] || '');
const isDeepSeek = computed(() => formState.value.provider_type === DEEPSEEK_PROVIDER);

const providerEndpoints: Record<string, string> = {
  openai: 'https://api.openai.com/v1/chat/completions',
  deepseek: DEEPSEEK_CANONICAL_ENDPOINT,
  qwen: 'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions',
  zhipu: 'https://open.bigmodel.cn/api/paas/v4/chat/completions',
  moonshot: 'https://api.moonshot.cn/v1/chat/completions',
  baichuan: 'https://api.baichuan-ai.com/v1/chat/completions',
  minimax: 'https://api.minimax.chat/v1/text/chatcompletion_v2',
  local: 'http://localhost:11434/v1/chat/completions',
};

const extractErrorMessage = (error: any): string => {
  const detail = error?.response?.data?.detail;
  if (typeof detail === 'string') {
    return detail;
  }
  return detail?.error || detail?.message || error?.response?.data?.error || error?.response?.data?.message || error?.message || '未知错误';
};

const mergeTestStates = (items: NormalizedLLMResource[]) =>
  items.map((item) => {
    const state = testStates.value[item.name];
    return {
      ...item,
      lastTestStatus: state?.status ?? item.lastTestStatus,
      lastTestError: state?.error ?? item.lastTestError,
    };
  });

const normalizeProviderType = (provider: string | undefined): string => {
  const normalized = String(provider || '').trim().toLowerCase();
  return providerEndpoints[normalized] ? normalized : 'openai';
};

const normalizeDeepseekEndpoint = (endpoint: string): string => {
  const trimmed = String(endpoint || '').trim();
  if (!trimmed) {
    return DEEPSEEK_CANONICAL_ENDPOINT;
  }
  try {
    const url = new URL(trimmed);
    const path = (url.pathname || '').replace(/\/+$/, '').toLowerCase();
    if (!path || path === '/' || path === '/v1' || path === '/v1/chat/completions' || path === '/chat/completions') {
      return `${url.origin}/chat/completions`;
    }
    return trimmed;
  } catch {
    return trimmed;
  }
};

const normalizeEndpointForProvider = (provider: string, endpoint: string): string => {
  if (provider === DEEPSEEK_PROVIDER) {
    return normalizeDeepseekEndpoint(endpoint);
  }
  return String(endpoint || '').trim();
};

const normalizeTemperatureForPayload = (temperature: unknown): number | undefined => {
  if (temperature === undefined || temperature === null || temperature === '') {
    return undefined;
  }
  const parsed = Number(temperature);
  if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) {
    return undefined;
  }
  return parsed;
};

const normalizeMaxTokensForPayload = (maxTokens: unknown): number | undefined => {
  if (maxTokens === undefined || maxTokens === null || maxTokens === '') {
    return undefined;
  }
  const parsed = Number(maxTokens);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return undefined;
  }
  return Math.floor(parsed);
};

const handleProviderChange = (provider: string) => {
  const normalizedProvider = normalizeProviderType(provider);
  formState.value.provider_type = normalizedProvider;
  if (providerEndpoints[normalizedProvider]) {
    formState.value.endpoint = providerEndpoints[normalizedProvider];
  }
  if (normalizedProvider === DEEPSEEK_PROVIDER && !formState.value.model_name) {
    formState.value.model_name = 'deepseek-v4-pro';
  }
};

const loadResources = async () => {
  loading.value = true;
  try {
    const response = await dorisApi.llm.list();
    const normalized = (response.data.resources || []).map((resource: any) => normalizeLLMResource(resource));
    resources.value = mergeTestStates(normalized);
  } catch (error: any) {
    message.error('加载失败: ' + extractErrorMessage(error));
  } finally {
    loading.value = false;
  }
};

const validateForm = () => {
  if (!formState.value.resource_name || !formState.value.provider_type ||
      !formState.value.endpoint || !formState.value.model_name) {
    message.warning('请填写必填项');
    return false;
  }
  return true;
};

const buildPayload = (): LLMConfigRequest => {
  const providerType = normalizeProviderType(formState.value.provider_type);
  const temperature = normalizeTemperatureForPayload(formState.value.temperature);
  const maxTokens = normalizeMaxTokensForPayload(formState.value.max_tokens);
  const payload: LLMConfigRequest = {
    resource_name: formState.value.resource_name.trim(),
    provider_type: providerType,
    endpoint: normalizeEndpointForProvider(providerType, formState.value.endpoint),
    model_name: formState.value.model_name.trim(),
  };
  if (temperature !== undefined) {
    payload.temperature = temperature;
  }
  if (maxTokens !== undefined) {
    payload.max_tokens = maxTokens;
  }
  const apiKey = formState.value.api_key.trim();
  if (!isEditing.value || apiKey) {
    payload.api_key = apiKey || undefined;
  }
  return payload;
};

const persistForm = async () => {
  if (!validateForm()) {
    return false;
  }
  const payload = buildPayload();
  if (isEditing.value && editingResourceName.value) {
    await dorisApi.llm.update(editingResourceName.value, payload);
  } else {
    await dorisApi.llm.create(payload);
    editingResourceName.value = payload.resource_name;
  }
  await loadResources();
  return true;
};

const openCreateModal = () => {
  editingResourceName.value = null;
  formState.value = defaultFormState();
  modalTestMessage.value = '';
  showModal.value = true;
};

const openEditModal = (record: NormalizedLLMResource) => {
  const normalizedProvider = normalizeProviderType(record.provider);
  editingResourceName.value = record.name;
  formState.value = {
    resource_name: record.name,
    provider_type: normalizedProvider,
    endpoint: normalizeEndpointForProvider(normalizedProvider, record.endpoint),
    model_name: record.model,
    api_key: '',
    temperature: normalizeTemperatureForPayload(record.temperature),
    max_tokens: record.maxTokens,
  };
  const state = testStates.value[record.name];
  modalTestAlertType.value = state?.status === 'failed' ? 'error' : 'info';
  modalTestMessage.value = state?.error ? `最近一次连接测试失败：${state.error}` : '';
  showModal.value = true;
};

const handleSubmit = async () => {
  submitting.value = true;
  const wasEditing = isEditing.value;
  try {
    const saved = await persistForm();
    if (!saved) {
      return;
    }
    message.success(wasEditing ? '配置更新成功' : '配置创建成功');
    resetForm();
  } catch (error: any) {
    message.error('保存失败: ' + extractErrorMessage(error));
  } finally {
    submitting.value = false;
  }
};

const handleSaveAndTest = async () => {
  testing.value = true;
  modalTestMessage.value = '';
  let saved = false;
  try {
    saved = await persistForm();
    if (!saved) {
      return;
    }
    const resourceName = formState.value.resource_name.trim();
    const response = await dorisApi.llm.test(resourceName);
    if (response.data.success) {
      testStates.value[resourceName] = { status: 'success' };
      modalTestAlertType.value = 'success';
      modalTestMessage.value = '配置已保存，连接测试成功';
      message.success('配置已保存，连接测试成功');
    } else {
      const errorText = response.data.error || response.data.message || '连接测试失败';
      testStates.value[resourceName] = { status: 'failed', error: errorText };
      modalTestAlertType.value = 'error';
      modalTestMessage.value = `配置已保存，连接测试失败：${errorText}`;
    }
    await loadResources();
  } catch (error: any) {
    if (!saved) {
      message.error('保存失败: ' + extractErrorMessage(error));
      return;
    }
    const resourceName = formState.value.resource_name.trim();
    const errorText = extractErrorMessage(error);
    if (resourceName) {
      testStates.value[resourceName] = { status: 'failed', error: errorText };
    }
    modalTestAlertType.value = 'error';
    modalTestMessage.value = `配置已保存，连接测试失败：${errorText}`;
    await loadResources();
  } finally {
    testing.value = false;
  }
};

const handleDelete = async (resourceName: string) => {
  try {
    await dorisApi.llm.delete(resourceName);
    delete testStates.value[resourceName];
    message.success('删除成功');
    loadResources();
  } catch (error: any) {
    message.error('删除失败: ' + extractErrorMessage(error));
  }
};

const resetForm = () => {
  showModal.value = false;
  editingResourceName.value = null;
  modalTestMessage.value = '';
  modalTestAlertType.value = 'info';
  formState.value = defaultFormState();
};

const testStatusLabel = (status?: string) => {
  if (status === 'success') {
    return '成功';
  }
  if (status === 'failed') {
    return '失败';
  }
  return '未测试';
};

const testStatusColor = (status?: string) => {
  if (status === 'success') {
    return 'green';
  }
  if (status === 'failed') {
    return 'red';
  }
  return 'default';
};

onMounted(() => {
  loadResources();
});
</script>

<style scoped>
.llm-config {
  padding: 24px;
}

.endpoint-text {
  display: inline-block;
  max-width: 280px;
  overflow: hidden;
  text-overflow: ellipsis;
  vertical-align: bottom;
  white-space: nowrap;
}

.error-summary {
  color: #cf1322;
  display: inline-block;
  max-width: 200px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.field-hint {
  color: #666;
  font-size: 12px;
  margin-top: 6px;
  line-height: 1.5;
}

.warning-text {
  color: #d46b08;
}
</style>
