<template>
  <div class="llm-config">
    <a-card title="LLM 配置管理">
      <a-button type="primary" @click="showModal = true" style="margin-bottom: 16px">
        添加 LLM 配置
      </a-button>

      <a-table
        :columns="columns"
        :data-source="resources"
        :loading="loading"
        row-key="ResourceName"
      >
        <template #bodyCell="{ column, record }">
          <template v-if="column.key === 'action'">
            <a-space>
              <a-button size="small" @click="handleTest(record.ResourceName)">测试</a-button>
              <a-popconfirm
                title="确定删除此配置?"
                @confirm="handleDelete(record.ResourceName)"
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
      title="添加 LLM 配置"
      @ok="handleSubmit"
      @cancel="resetForm"
      :confirm-loading="submitting"
      width="600px"
    >
      <a-form :model="formState" layout="vertical">
        <a-form-item label="资源名称" required>
          <a-input v-model:value="formState.resource_name" placeholder="例如: my_openai" />
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

        <a-form-item label="API 端点" required>
          <a-input v-model:value="formState.endpoint" :placeholder="endpointPlaceholder" />
        </a-form-item>

        <a-form-item label="模型名称" required>
          <a-input v-model:value="formState.model_name" placeholder="gpt-4" />
        </a-form-item>

        <a-form-item label="API 密钥">
          <a-input-password v-model:value="formState.api_key" placeholder="sk-xxxxx (可选)" />
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
    </a-modal>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue';
import { message } from 'ant-design-vue';
import { dorisApi } from '../api/doris';

const columns = [
  { title: '资源名称', dataIndex: 'ResourceName', key: 'ResourceName' },
  { title: '类型', dataIndex: 'ResourceType', key: 'ResourceType' },
  { title: '操作', key: 'action', width: 200 },
];

const resources = ref([]);
const loading = ref(false);
const showModal = ref(false);
const submitting = ref(false);

const formState = ref({
  resource_name: '',
  provider_type: 'openai',
  endpoint: '',
  model_name: '',
  api_key: '',
  temperature: undefined,
  max_tokens: undefined,
});

const endpointPlaceholder = ref('https://api.openai.com/v1/chat/completions');

// 厂商默认端点映射
const providerEndpoints: Record<string, string> = {
  openai: 'https://api.openai.com/v1/chat/completions',
  deepseek: 'https://api.deepseek.com/chat/completions',
  qwen: 'https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions',
  zhipu: 'https://open.bigmodel.cn/api/paas/v4/chat/completions',
  moonshot: 'https://api.moonshot.cn/v1/chat/completions',
  baichuan: 'https://api.baichuan-ai.com/v1/chat/completions',
  minimax: 'https://api.minimax.chat/v1/text/chatcompletion_v2',
  local: 'http://localhost:11434/v1/chat/completions',
};

const handleProviderChange = (provider: string) => {
  // 自动填充默认端点
  if (providerEndpoints[provider]) {
    formState.value.endpoint = providerEndpoints[provider];
    endpointPlaceholder.value = providerEndpoints[provider];
  }
};

const loadResources = async () => {
  loading.value = true;
  try {
    const response = await dorisApi.llm.list();
    resources.value = response.data.resources;
  } catch (error: any) {
    message.error('加载失败: ' + (error.response?.data?.detail || error.message));
  } finally {
    loading.value = false;
  }
};

const handleSubmit = async () => {
  if (!formState.value.resource_name || !formState.value.provider_type || 
      !formState.value.endpoint || !formState.value.model_name) {
    message.warning('请填写必填项');
    return;
  }

  submitting.value = true;
  try {
    await dorisApi.llm.create(formState.value);
    message.success('配置创建成功');
    showModal.value = false;
    resetForm();
    loadResources();
  } catch (error: any) {
    message.error('创建失败: ' + (error.response?.data?.detail?.error || error.message));
  } finally {
    submitting.value = false;
  }
};

const handleTest = async (resourceName: string) => {
  try {
    const response = await dorisApi.llm.test(resourceName);
    if (response.data.success) {
      message.success('测试成功! LLM 连接正常');
    } else {
      message.error('测试失败: ' + response.data.error);
    }
  } catch (error: any) {
    message.error('测试失败: ' + (error.response?.data?.detail?.error || error.message));
  }
};

const handleDelete = async (resourceName: string) => {
  try {
    await dorisApi.llm.delete(resourceName);
    message.success('删除成功');
    loadResources();
  } catch (error: any) {
    message.error('删除失败: ' + (error.response?.data?.detail || error.message));
  }
};

const resetForm = () => {
  formState.value = {
    resource_name: '',
    provider_type: 'openai',
    endpoint: '',
    model_name: '',
    api_key: '',
    temperature: undefined,
    max_tokens: undefined,
  };
};

onMounted(() => {
  loadResources();
});
</script>

<style scoped>
.llm-config {
  padding: 24px;
}
</style>

