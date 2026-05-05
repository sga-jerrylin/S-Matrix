<template>
  <a-config-provider :theme="{ token: { colorPrimary: '#1890ff' } }">
    <a-layout style="min-height: 100vh">
      <a-layout-header style="background: #001529; padding: 0">
        <div style="display: flex; align-items: center; padding: 0 24px">
          <h1 style="color: white; margin: 0; font-size: 20px">
            Doris 数据中台
          </h1>
          <a-space style="margin-left: auto">
            <a-tag :color="gatewayKeyStatus.color">{{ gatewayKeyStatus.text }}</a-tag>
            <a-button size="small" @click="openGatewayKeyModal">
              <key-outlined />
              网关 Key
            </a-button>
            <a-badge :status="healthStatus" :text="healthText" />
          </a-space>
        </div>
      </a-layout-header>

      <a-layout>
        <a-layout-sider width="200" style="background: #fff">
          <a-menu
            v-model:selected-keys="selectedKeys"
            mode="inline"
            style="height: 100%; border-right: 0"
          >
            <a-menu-item key="upload">
              <upload-outlined />
              <span>准备数据 (上传)</span>
            </a-menu-item>
            <a-menu-item key="query">
              <search-outlined />
              <span>数据查询 (查询)</span>
            </a-menu-item>
            <a-menu-item key="analysis">
              <fund-outlined />
              <span>智能洞察 (洞察)</span>
            </a-menu-item>
            <a-menu-item key="forecast">
              <line-chart-outlined />
              <span>业务预测 (预测)</span>
            </a-menu-item>
            <a-divider style="margin: 8px 0" />
            <a-menu-item key="llm">
              <api-outlined />
              <span>系统设置 (LLM)</span>
            </a-menu-item>
          </a-menu>
        </a-layout-sider>

        <a-layout-content style="background: #f0f2f5; padding: 24px; min-height: 280px">
          <WorkbenchUpload v-if="selectedKeys[0] === 'upload'" />
          <WorkbenchQuery v-if="selectedKeys[0] === 'query'" />
          <DataAnalysis v-if="selectedKeys[0] === 'analysis'" />
          <WorkbenchForecast v-if="selectedKeys[0] === 'forecast'" />
          <LLMConfig v-if="selectedKeys[0] === 'llm'" />
        </a-layout-content>
      </a-layout>
    </a-layout>

    <a-modal
      v-model:open="gatewayKeyModalOpen"
      title="设置网关 API Key"
      @ok="saveGatewayKey"
      ok-text="保存并刷新"
    >
      <a-alert
        type="info"
        show-icon
        style="margin-bottom: 16px"
        message="Docker 部署会通过前端代理自动带认证。这里用于本地开发或手动覆盖当前浏览器的 API Key。"
      />
      <a-form layout="vertical">
        <a-form-item label="API Key">
          <a-input-password
            v-model:value="gatewayKeyInput"
            placeholder="留空则清除浏览器本地覆盖值"
          />
        </a-form-item>
      </a-form>
    </a-modal>
  </a-config-provider>
</template>

<script setup lang="ts">
import { computed, ref, onMounted } from 'vue';
import { message } from 'ant-design-vue';
import { UploadOutlined, ApiOutlined, SearchOutlined, RobotOutlined, CloudSyncOutlined, TableOutlined, KeyOutlined, FundOutlined, LineChartOutlined } from '@ant-design/icons-vue';
import LLMConfig from './components/LLMConfig.vue';
import DataAnalysis from './components/DataAnalysis.vue';
import WorkbenchUpload from './components/WorkbenchUpload.vue';
import WorkbenchQuery from './components/WorkbenchQuery.vue';
import WorkbenchForecast from './components/WorkbenchForecast.vue';
import { dorisApi } from './api/doris';
import { clearStoredGatewayApiKey, resolveGatewayApiKey, setStoredGatewayApiKey } from './api/auth';

const selectedKeys = ref(['upload']);
const healthStatus = ref<'success' | 'error' | 'default'>('default');
const healthText = ref('检查中...');
const gatewayKeyModalOpen = ref(false);
const gatewayKeyInput = ref('');
const usesProxyGatewayAuth = !import.meta.env.VITE_API_BASE_URL;

const gatewayKeyStatus = computed(() => {
  const localOverrideKey = resolveGatewayApiKey();
  if (localOverrideKey) {
    return { color: 'green', text: '浏览器 Key 已覆盖' };
  }
  if (usesProxyGatewayAuth) {
    return { color: 'blue', text: '代理认证模式' };
  }
  return { color: 'orange', text: '网关 Key 未配置' };
});

const checkHealth = async () => {
  try {
    const response = await dorisApi.health();
    if (response.data.doris_connected) {
      healthStatus.value = 'success';
      healthText.value = 'Doris 已连接';
    } else {
      healthStatus.value = 'error';
      healthText.value = 'Doris 未连接';
    }
  } catch (error) {
    healthStatus.value = 'error';
    healthText.value = 'API 服务异常';
  }
};

const openGatewayKeyModal = () => {
  gatewayKeyInput.value = resolveGatewayApiKey({
    envApiKey: import.meta.env.VITE_SMATRIX_API_KEY,
  });
  gatewayKeyModalOpen.value = true;
};

const saveGatewayKey = async () => {
  const nextValue = gatewayKeyInput.value.trim();
  if (nextValue) {
    setStoredGatewayApiKey(nextValue);
    message.success('已保存浏览器本地网关 Key，页面将刷新');
  } else {
    clearStoredGatewayApiKey();
    message.success('已清除浏览器本地网关 Key，页面将刷新');
  }

  gatewayKeyModalOpen.value = false;
  await checkHealth();
  window.location.reload();
};

const autoInitApiKey = async () => {
  // 已有 key（localStorage 或 env）则跳过
  const existing = resolveGatewayApiKey({ envApiKey: import.meta.env.VITE_SMATRIX_API_KEY });
  if (existing) return;

  try {
    const res = await fetch('/api/config');
    if (!res.ok) return;
    const data = await res.json();
    if (data.api_key) {
      setStoredGatewayApiKey(data.api_key);
    }
  } catch {
    // 静默失败，不影响页面加载
  }
};

onMounted(async () => {
  await autoInitApiKey();
  checkHealth();
  setInterval(checkHealth, 30000);
});
</script>

<style>
#app {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial,
    'Noto Sans', sans-serif;
}
</style>
