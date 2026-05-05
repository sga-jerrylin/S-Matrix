<template>
  <div class="workbench-query">
    <a-tabs v-model:activeKey="activeTab" type="card">
      <a-tab-pane key="natural" tab="自然语言查询">
        <NaturalQuery />
      </a-tab-pane>
      <a-tab-pane key="structured" tab="高级查询与分析">
        <DataQuery />
      </a-tab-pane>
    </a-tabs>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue';
import NaturalQuery from './NaturalQuery.vue';
import DataQuery from './DataQuery.vue';

const activeTab = ref('natural');
</script>

<style scoped>
.workbench-query {
  background: white;
  padding: 16px 24px;
  border-radius: 8px;
}
</style>
