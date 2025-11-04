import axios from 'axios';

// 使用相对路径,通过 Nginx 代理到后端
// 开发环境: http://localhost:8000
// 生产环境(Docker): /api (由 Nginx 代理)
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '';

const api = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

export interface ExecuteRequest {
  action: string;
  table?: string;
  column?: string;
  params?: Record<string, any>;
}

export interface LLMConfigRequest {
  resource_name: string;
  provider_type: string;
  endpoint: string;
  model_name: string;
  api_key?: string;
  temperature?: number;
  max_tokens?: number;
}

export interface NaturalQueryRequest {
  query: string;
  api_key?: string;
  model?: string;
  base_url?: string;
}

export const dorisApi = {
  // 健康检查
  health: () => api.get('/api/health'),

  // 获取所有表
  getTables: () => api.get('/api/tables'),

  // 获取表结构
  getTableSchema: (tableName: string) => api.get(`/api/tables/${tableName}/schema`),

  // 执行操作
  execute: (data: ExecuteRequest) => api.post('/api/execute', data),

  // Excel 上传预览
  previewExcel: (file: File, rows: number = 10) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('rows', rows.toString());
    return api.post('/api/upload/preview', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },

  // Excel 上传导入
  uploadExcel: (file: File, tableName: string, columnMapping?: Record<string, string>, createTable: boolean = true) => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('table_name', tableName);
    if (columnMapping) {
      formData.append('column_mapping', JSON.stringify(columnMapping));
    }
    formData.append('create_table', createTable.toString());
    return api.post('/api/upload', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
  },

  // LLM 配置管理
  llm: {
    create: (data: LLMConfigRequest) => api.post('/api/llm/config', data),
    list: () => api.get('/api/llm/config'),
    test: (resourceName: string) => api.post(`/api/llm/config/${resourceName}/test`),
    delete: (resourceName: string) => api.delete(`/api/llm/config/${resourceName}`),
  },

  // 自然语言查询 (Text-to-SQL)
  naturalQuery: (data: NaturalQueryRequest) => api.post('/api/query/natural', data),
};

