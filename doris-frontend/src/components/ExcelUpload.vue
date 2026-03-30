<template>
  <div class="excel-upload">
    <a-card title="Excel 文件上传">
      <a-upload-dragger
        v-model:file-list="fileList"
        :before-upload="beforeUpload"
        :max-count="1"
        accept=".xlsx,.xls"
        @change="handleFileChange"
      >
        <p class="ant-upload-drag-icon">
          <inbox-outlined />
        </p>
        <p class="ant-upload-text">点击或拖拽 Excel 文件到此区域</p>
        <p class="ant-upload-hint">支持 .xlsx 和 .xls 格式</p>
      </a-upload-dragger>

      <a-divider />

      <a-form :model="formState" layout="vertical">
        <a-form-item label="目标表名" required>
          <a-input v-model:value="formState.tableName" placeholder="输入表名" />
        </a-form-item>

        <a-form-item label="导入方式" required>
          <a-radio-group v-model:value="formState.importMode">
            <a-radio-button value="replace">覆盖现有表</a-radio-button>
            <a-radio-button value="append">追加到现有表</a-radio-button>
          </a-radio-group>
          <div class="form-hint">
            覆盖会重建同名表；追加要求上传文件与现有表的列名和顺序完全一致。
          </div>
        </a-form-item>

        <a-form-item>
          <a-checkbox v-model:checked="formState.createTable">
            如果表不存在则自动创建
          </a-checkbox>
        </a-form-item>

        <a-form-item>
          <a-space>
            <a-button type="primary" @click="handlePreview" :loading="previewLoading" :disabled="!currentFile">
              预览数据
            </a-button>
            <a-button type="primary" @click="handleUpload" :loading="uploadLoading" :disabled="!currentFile || !formState.tableName">
              上传导入
            </a-button>
          </a-space>
        </a-form-item>
      </a-form>

      <a-divider v-if="previewData" />

      <div v-if="previewData">
        <h3>数据预览 (前 {{ previewData.row_count }} 行)</h3>
        <a-table
          :columns="previewColumns"
          :data-source="previewData.data"
          :pagination="false"
          size="small"
          :scroll="{ x: 'max-content' }"
        />
        
        <a-divider />
        
        <h4>推断的列类型</h4>
        <a-descriptions bordered size="small" :column="2">
          <a-descriptions-item
            v-for="(type, col) in previewData.inferred_types"
            :key="col"
            :label="col"
          >
            {{ type }}
          </a-descriptions-item>
        </a-descriptions>
      </div>
    </a-card>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue';
import { message } from 'ant-design-vue';
import { InboxOutlined } from '@ant-design/icons-vue';
import { dorisApi, type UploadImportMode } from '../api/doris';
import { extractApiErrorMessage } from '../api/errors';

const fileList = ref([]);
const currentFile = ref<File | null>(null);
const previewLoading = ref(false);
const uploadLoading = ref(false);
const previewData = ref<any>(null);

const formState = ref({
  tableName: '',
  importMode: 'replace' as UploadImportMode,
  createTable: true,
});

const beforeUpload = (file: File) => {
  currentFile.value = file;
  return false;
};

const handleFileChange = (info: any) => {
  if (info.fileList.length === 0) {
    currentFile.value = null;
    previewData.value = null;
  }
};

const previewColumns = computed(() => {
  if (!previewData.value || !previewData.value.columns) return [];
  return previewData.value.columns.map((col: string) => ({
    title: col,
    dataIndex: col,
    key: col,
    ellipsis: true,
  }));
});

const handlePreview = async () => {
  if (!currentFile.value) {
    message.warning('请先选择文件');
    return;
  }

  previewLoading.value = true;
  try {
    const response = await dorisApi.previewExcel(currentFile.value, 10);
    previewData.value = response.data;
    message.success('预览成功');
  } catch (error: any) {
    message.error('预览失败: ' + extractApiErrorMessage(error));
  } finally {
    previewLoading.value = false;
  }
};

const handleUpload = async () => {
  if (!currentFile.value) {
    message.warning('请先选择文件');
    return;
  }

  if (!formState.value.tableName) {
    message.warning('请输入表名');
    return;
  }

  uploadLoading.value = true;
  try {
    const response = await dorisApi.uploadExcel(
      currentFile.value,
      formState.value.tableName,
      undefined,
      formState.value.createTable,
      formState.value.importMode,
    );
    const actionLabel = response.data.table_replaced ? '覆盖导入' : '导入';
    message.success(`成功${actionLabel} ${response.data.rows_imported} 行数据到表 ${response.data.table}`);
    
    fileList.value = [];
    currentFile.value = null;
    previewData.value = null;
    formState.value.tableName = '';
    formState.value.importMode = 'replace';
  } catch (error: any) {
    message.error('上传失败: ' + extractApiErrorMessage(error));
  } finally {
    uploadLoading.value = false;
  }
};
</script>

<style scoped>
.excel-upload {
  padding: 24px;
}

.form-hint {
  margin-top: 8px;
  color: #999;
  font-size: 12px;
}
</style>
