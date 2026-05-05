<template>
  <div class="workbench-upload">
    <a-tabs v-model:activeKey="activeTab" type="card" @change="onTabChange">
      <a-tab-pane key="datasource" tab="数据源同步">
        <DataSourceSync
          :local-table-count="registryCount"
          @view-registry="activeTab = 'registry'"
          @registry-count-loaded="setRegistryCount"
        />
      </a-tab-pane>
      <a-tab-pane key="registry" :tab="`已同步表格 (${registryCount})`">
        <TableRegistry @count-loaded="setRegistryCount" />
      </a-tab-pane>
      <a-tab-pane key="excel" tab="Excel上传">
        <ExcelUpload />
      </a-tab-pane>
    </a-tabs>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue';
import DataSourceSync from './DataSourceSync.vue';
import TableRegistry from './TableRegistry.vue';
import ExcelUpload from './ExcelUpload.vue';
import { dorisApi } from '../api/doris';

const activeTab = ref('datasource');
const registryCount = ref(0);
const userHasSwitchedTab = ref(false);

const applyDefaultTab = (count: number) => {
  if (userHasSwitchedTab.value) return;
  activeTab.value = count > 0 ? 'registry' : 'datasource';
};

const onTabChange = (key: string) => {
  activeTab.value = key;
  userHasSwitchedTab.value = true;
};

const setRegistryCount = (count: number) => {
  registryCount.value = count;
  applyDefaultTab(count);
};

const loadRegistryCount = async () => {
  try {
    const response = await dorisApi.tableRegistry.list();
    const count = (response.data.tables || []).length;
    setRegistryCount(count);
  } catch {
    setRegistryCount(0);
  }
};

onMounted(() => {
  loadRegistryCount();
});
</script>

<style scoped>
.workbench-upload {
  background: white;
  padding: 16px 24px;
  border-radius: 8px;
}
</style>
