<template>
  <div class="datasource-sync">
    <a-row :gutter="24">
      <!-- 左侧：数据源配置 -->
      <a-col :span="10">
        <a-card title="数据源配置" :bordered="false">
          <a-form :model="formState" layout="vertical">
            <a-form-item label="数据源名称">
              <a-input v-model:value="formState.name" placeholder="如：生产数据库" />
            </a-form-item>
            <a-form-item label="主机地址 (Host)">
              <a-input v-model:value="formState.host" placeholder="如：192.168.1.100" />
            </a-form-item>
            <a-form-item label="端口 (Port)">
              <a-input-number v-model:value="formState.port" :min="1" :max="65535" style="width: 100%" />
            </a-form-item>
            <a-form-item label="用户名 (User)">
              <a-input v-model:value="formState.user" placeholder="数据库用户名" />
            </a-form-item>
            <a-form-item label="密码 (Password)">
              <a-input-password v-model:value="formState.password" placeholder="数据库密码" />
            </a-form-item>
            <a-form-item label="数据库名">
              <a-select 
                v-model:value="formState.database" 
                :options="databaseOptions"
                placeholder="先测试连接获取数据库列表"
                :disabled="databases.length === 0"
              />
            </a-form-item>
            <a-form-item>
              <a-space>
                <a-button type="primary" ghost @click="testConnection" :loading="testLoading">
                  测试连接
                </a-button>
                <a-button type="primary" @click="saveDataSource" :loading="saveLoading" :disabled="!formState.database">
                  保存数据源
                </a-button>
              </a-space>
            </a-form-item>
          </a-form>

          <!-- 已保存的数据源列表 -->
          <a-divider>已保存的数据源</a-divider>
          <a-list :data-source="datasources" :loading="listLoading" size="small">
            <template #renderItem="{ item }">
              <a-list-item>
                <a-list-item-meta>
                  <template #title>
                    <a @click="selectDataSource(item)">{{ item.name }}</a>
                  </template>
                  <template #description>
                    {{ item.host }}:{{ item.port }} / {{ item.database_name }}
                  </template>
                </a-list-item-meta>
                <template #actions>
                  <a-button type="link" size="small" @click="selectDataSource(item)">选择</a-button>
                  <a-popconfirm title="确定删除此数据源？" @confirm="deleteDataSource(item.id)">
                    <a-button type="link" danger size="small">删除</a-button>
                  </a-popconfirm>
                </template>
              </a-list-item>
            </template>
          </a-list>
        </a-card>
      </a-col>

      <!-- 右侧：表格同步 -->
      <a-col :span="14">
        <a-card title="表格同步" :bordered="false">
          <template #extra>
            <a-tag v-if="selectedDatasource" color="blue">
              当前: {{ selectedDatasource.name }}
            </a-tag>
          </template>

          <div v-if="!selectedDatasource" style="text-align: center; padding: 40px; color: #999">
            请先选择或创建一个数据源
          </div>

          <div v-else>
            <!-- 表格选择 -->
            <a-spin :spinning="tablesLoading">
              <a-checkbox-group v-model:value="selectedTables" style="width: 100%">
                <a-row :gutter="[16, 8]">
                  <a-col :span="8" v-for="table in remoteTables" :key="table.name">
                    <a-checkbox :value="table.name">
                      {{ table.name }}
                      <a-tag size="small" v-if="table.row_count">{{ table.row_count }} 行</a-tag>
                    </a-checkbox>
                  </a-col>
                </a-row>
              </a-checkbox-group>
            </a-spin>

            <a-divider />

            <!-- 同步配置 -->
            <a-form layout="inline" style="margin-bottom: 16px">
              <a-form-item label="同步方式">
                <a-radio-group v-model:value="syncMode">
                  <a-radio value="immediate">立即同步</a-radio>
                  <a-radio value="scheduled">定时同步</a-radio>
                </a-radio-group>
              </a-form-item>
              <a-form-item v-if="syncMode === 'scheduled'" label="同步周期">
                <a-select v-model:value="scheduleType" style="width: 120px">
                  <a-select-option value="hourly">每小时</a-select-option>
                  <a-select-option value="daily">每天</a-select-option>
                  <a-select-option value="weekly">每周</a-select-option>
                </a-select>
              </a-form-item>
            </a-form>

            <a-button 
              type="primary" 
              size="large" 
              block 
              @click="startSync" 
              :loading="syncLoading"
              :disabled="selectedTables.length === 0"
            >
              <sync-outlined /> 开始同步 ({{ selectedTables.length }} 张表)
            </a-button>

            <!-- 同步结果 -->
            <a-alert
              v-if="syncResult"
              :type="syncResult.success ? 'success' : 'error'"
              :message="syncResult.success ? '同步完成' : '同步失败'"
              :description="syncResultDescription"
              style="margin-top: 16px"
              show-icon
              closable
            />
          </div>
        </a-card>
      </a-col>
    </a-row>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue';
import { message } from 'ant-design-vue';
import { SyncOutlined } from '@ant-design/icons-vue';
import { dorisApi } from '../api/doris';

// 表单状态
const formState = ref({
  name: '',
  host: '',
  port: 3306,
  user: '',
  password: '',
  database: ''
});

// 数据
const databases = ref<string[]>([]);
const datasources = ref<any[]>([]);
const selectedDatasource = ref<any>(null);
const remoteTables = ref<any[]>([]);
const selectedTables = ref<string[]>([]);

// 同步配置
const syncMode = ref('immediate');
const scheduleType = ref('daily');
const syncResult = ref<any>(null);

// Loading 状态
const testLoading = ref(false);
const saveLoading = ref(false);
const listLoading = ref(false);
const tablesLoading = ref(false);
const syncLoading = ref(false);

// 计算属性
const databaseOptions = computed(() =>
  databases.value.map(db => ({ label: db, value: db }))
);

const syncResultDescription = computed(() => {
  if (!syncResult.value) return '';
  if (syncResult.value.success) {
    return `成功同步 ${syncResult.value.success_count} 张表，共 ${syncResult.value.results?.reduce((sum: number, r: any) => sum + (r.rows_synced || 0), 0) || 0} 行数据`;
  }
  
  // 辅助函数：安全地将错误转换为字符串
  const formatError = (err: any): string => {
    if (!err) return '';
    if (typeof err === 'string') return err;
    if (typeof err === 'object') {
      return err.detail || err.error || err.message || JSON.stringify(err);
    }
    return String(err);
  };

  // 优先显示后端返回的顶层错误信息
  if (syncResult.value.error) {
    return formatError(syncResult.value.error);
  }
  
  // 尝试从 results 中查找失败的表
  if (syncResult.value.results && syncResult.value.results.length > 0) {
    const failed = syncResult.value.results.find((r: any) => !r.success);
    if (failed) {
      return `同步失败: ${formatError(failed.error) || '未知错误'}`;
    }
  }
  
  // 如果还是没有找到错误信息，显示原始数据的简减版
  return `未知错误 (原始数据: ${JSON.stringify(syncResult.value).substring(0, 200)}...)`;
});

// 测试连接
const testConnection = async () => {
  if (!formState.value.host || !formState.value.user) {
    message.warning('请填写主机地址和用户名');
    return;
  }

  testLoading.value = true;
  try {
    const response = await dorisApi.datasource.test({
      host: formState.value.host,
      port: formState.value.port,
      user: formState.value.user,
      password: formState.value.password
    });

    if (response.data.success) {
      message.success('连接成功！');
      databases.value = response.data.databases || [];
    } else {
      message.error(response.data.message || '连接失败');
    }
  } catch (error: any) {
    message.error('连接失败: ' + (error.response?.data?.detail || error.message));
  } finally {
    testLoading.value = false;
  }
};

// 保存数据源
const saveDataSource = async () => {
  if (!formState.value.name || !formState.value.database) {
    message.warning('请填写数据源名称和选择数据库');
    return;
  }

  saveLoading.value = true;
  try {
    await dorisApi.datasource.save(formState.value);
    message.success('数据源保存成功');
    loadDatasources();
    // 清空表单
    formState.value = { name: '', host: '', port: 3306, user: '', password: '', database: '' };
    databases.value = [];
  } catch (error: any) {
    message.error('保存失败: ' + (error.response?.data?.detail || error.message));
  } finally {
    saveLoading.value = false;
  }
};

// 加载数据源列表
const loadDatasources = async () => {
  listLoading.value = true;
  try {
    const response = await dorisApi.datasource.list();
    datasources.value = response.data.datasources || [];
  } catch (error) {
    console.error('加载数据源失败', error);
  } finally {
    listLoading.value = false;
  }
};

// 选择数据源
const selectDataSource = async (ds: any) => {
  selectedDatasource.value = ds;
  selectedTables.value = [];
  syncResult.value = null;

  tablesLoading.value = true;
  try {
    const response = await dorisApi.datasource.getTables(ds.id);
    remoteTables.value = response.data.tables || [];
  } catch (error: any) {
    message.error('获取表列表失败: ' + (error.response?.data?.detail || error.message));
  } finally {
    tablesLoading.value = false;
  }
};

// 删除数据源
const deleteDataSource = async (dsId: string) => {
  try {
    await dorisApi.datasource.delete(dsId);
    message.success('数据源已删除');
    if (selectedDatasource.value?.id === dsId) {
      selectedDatasource.value = null;
      remoteTables.value = [];
    }
    loadDatasources();
  } catch (error: any) {
    message.error('删除失败: ' + (error.response?.data?.detail || error.message));
  }
};

// 开始同步
const startSync = async () => {
  if (!selectedDatasource.value || selectedTables.value.length === 0) {
    message.warning('请选择要同步的表');
    return;
  }

  syncLoading.value = true;
  syncResult.value = null;

  try {
    if (syncMode.value === 'scheduled') {
      // 定时同步：为每个表创建定时任务
      let successCount = 0;
      for (const tableName of selectedTables.value) {
        try {
          await dorisApi.syncTasks.create({
            datasource_id: selectedDatasource.value.id,
            source_table: tableName,
            schedule_type: scheduleType.value as 'hourly' | 'daily' | 'weekly'
          });
          successCount++;
        } catch (e) {
          console.error(`创建定时任务失败: ${tableName}`, e);
        }
      }
      syncResult.value = {
        success: successCount > 0,
        message: `已创建 ${successCount} 个定时同步任务（${scheduleType.value}）`
      };
      message.success(`已创建 ${successCount} 个定时同步任务`);
    } else {
      // 立即同步
      const tables = selectedTables.value.map(name => ({ source_table: name }));
      const response = await dorisApi.datasource.syncMultiple(selectedDatasource.value.id, tables);
      syncResult.value = response.data;

      if (response.data.success) {
        message.success(`同步完成！成功 ${response.data.success_count} 张表`);
      } else {
        message.warning(`同步部分失败：成功 ${response.data.success_count}，失败 ${response.data.fail_count}`);
      }
    }
  } catch (error: any) {
    syncResult.value = { success: false, error: error.response?.data?.detail || error.message };
    message.error('同步失败: ' + (error.response?.data?.detail || error.message));
  } finally {
    syncLoading.value = false;
  }
};

onMounted(() => {
  loadDatasources();
});
</script>

<style scoped>
.datasource-sync {
  padding: 24px;
}
</style>
