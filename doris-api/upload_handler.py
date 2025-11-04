"""
Excel 上传处理器
"""
import pandas as pd
import requests
from typing import Dict, Any, List
from io import BytesIO
from config import DORIS_STREAM_LOAD, DORIS_CONFIG
from db import doris_client


class ExcelUploadHandler:
    """Excel 上传和导入处理器"""
    
    def __init__(self):
        self.db = doris_client
        self.stream_load_config = DORIS_STREAM_LOAD
    
    def preview_excel(self, file_content: bytes, rows: int = 10) -> Dict[str, Any]:
        """
        预览 Excel 文件

        Args:
            file_content: 文件内容
            rows: 预览行数

        Returns:
            预览数据和列信息
        """
        import json
        import numpy as np

        df = pd.read_excel(BytesIO(file_content), nrows=rows)

        # 替换 NaN 和 Infinity 为 None (JSON null)
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.replace({np.nan: None})

        # 推断列类型
        column_types = {}
        for col in df.columns:
            dtype = df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                column_types[col] = 'INT'
            elif pd.api.types.is_float_dtype(dtype):
                column_types[col] = 'DECIMAL(18,2)'
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                column_types[col] = 'DATETIME'
            else:
                column_types[col] = 'VARCHAR(500)'

        # 转换为字典并确保 JSON 兼容
        data = json.loads(df.to_json(orient='records'))

        return {
            'columns': [str(col) for col in df.columns],
            'data': data,
            'row_count': len(df),
            'inferred_types': column_types
        }
    
    def create_table(self, table_name: str, columns: Dict[str, str], 
                     key_columns: List[str] = None) -> str:
        """
        创建表
        
        Args:
            table_name: 表名
            columns: 列定义 {列名: 类型}
            key_columns: 主键列 (可选,默认使用第一列)
        
        Returns:
            CREATE TABLE SQL
        """
        if not key_columns:
            key_columns = [list(columns.keys())[0]]
        
        # 构造列定义
        column_defs = []
        for col_name, col_type in columns.items():
            # 处理列名中的特殊字符
            safe_col_name = col_name.replace(' ', '_').replace('-', '_')
            column_defs.append(f"`{safe_col_name}` {col_type}")
        
        column_defs_str = ',\n    '.join(column_defs)
        key_columns_str = ', '.join([f"`{k}`" for k in key_columns])
        
        sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {column_defs_str}
        )
        DUPLICATE KEY({key_columns_str})
        DISTRIBUTED BY HASH({key_columns_str}) BUCKETS 10
        PROPERTIES (
            "replication_num" = "1"
        )
        """
        
        self.db.execute_update(sql)
        return sql
    
    def import_excel(self, file_content: bytes, table_name: str, 
                     column_mapping: Dict[str, str] = None,
                     create_table_if_not_exists: bool = True,
                     column_types: Dict[str, str] = None) -> Dict[str, Any]:
        """
        导入 Excel 到 Doris
        
        Args:
            file_content: 文件内容
            table_name: 目标表名
            column_mapping: 列映射 {Excel列名: Doris列名}
            create_table_if_not_exists: 如果表不存在是否创建
            column_types: 列类型定义 {列名: 类型}
        
        Returns:
            导入结果
        """
        import numpy as np

        # 读取 Excel
        df = pd.read_excel(BytesIO(file_content))

        # 替换 NaN 和 Infinity 为空字符串 (Doris 可以处理)
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.fillna('')

        # 应用列映射
        if column_mapping:
            df = df.rename(columns=column_mapping)

        # 清理列名
        df.columns = [col.replace(' ', '_').replace('-', '_') for col in df.columns]

        # 清理数据:移除字段中的换行符和制表符,避免 CSV 解析错误
        for col in df.columns:
            if df[col].dtype == 'object':  # 只处理字符串列
                df[col] = df[col].astype(str).str.replace('\n', ' ', regex=False)
                df[col] = df[col].str.replace('\r', ' ', regex=False)
                df[col] = df[col].str.replace('\t', ' ', regex=False)

        # 检查表是否存在
        table_exists = self.db.table_exists(table_name)

        if not table_exists:
            if not create_table_if_not_exists:
                raise ValueError(f"Table '{table_name}' does not exist")

            # 自动推断列类型
            if not column_types:
                column_types = {}
                for col in df.columns:
                    dtype = df[col].dtype
                    if pd.api.types.is_integer_dtype(dtype):
                        column_types[col] = 'BIGINT'
                    elif pd.api.types.is_float_dtype(dtype):
                        column_types[col] = 'DECIMAL(18,2)'
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        column_types[col] = 'DATETIME'
                    else:
                        column_types[col] = 'VARCHAR(500)'

            # 创建表
            create_sql = self.create_table(table_name, column_types)
        else:
            # 表已存在,检查列数是否匹配
            existing_schema = self.db.get_table_schema(table_name)

            if len(existing_schema) != len(df.columns):
                # 列数不匹配,删除旧表并重新创建
                self.db.execute_update(f"DROP TABLE `{table_name}`")

                # 自动推断列类型
                column_types = {}
                for col in df.columns:
                    dtype = df[col].dtype
                    if pd.api.types.is_integer_dtype(dtype):
                        column_types[col] = 'BIGINT'
                    elif pd.api.types.is_float_dtype(dtype):
                        column_types[col] = 'DECIMAL(18,2)'
                    elif pd.api.types.is_datetime64_any_dtype(dtype):
                        column_types[col] = 'DATETIME'
                    else:
                        column_types[col] = 'VARCHAR(500)'

                # 重新创建表
                create_sql = self.create_table(table_name, column_types)
                table_exists = False
        
        # 使用 Stream Load 导入数据
        result = self.stream_load(df, table_name)
        
        return {
            'success': True,
            'table': table_name,
            'rows_imported': len(df),
            'table_created': not table_exists,
            'stream_load_result': result
        }
    
    def stream_load(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        使用 Stream Load API 导入数据

        Args:
            df: Pandas DataFrame
            table_name: 目标表名

        Returns:
            Stream Load 响应
        """
        from io import BytesIO

        # 使用制表符作为分隔符,避免字段中的逗号导致列数错误
        # 确保所有列都被导出,包括空列
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False, header=False, encoding='utf-8', sep='\t', na_rep='')
        csv_bytes = csv_buffer.getvalue()

        # Stream Load URL
        url = f"http://{self.stream_load_config['host']}:{self.stream_load_config['port']}/api/{DORIS_CONFIG['database']}/{table_name}/_stream_load"

        # 请求头 (使用制表符作为列分隔符)
        headers = {
            'Expect': '100-continue',
            'Content-Type': 'text/plain; charset=utf-8',
            'format': 'csv',
            'column_separator': '\\t'
        }

        # 发送请求 (直接使用字节数据)
        response = requests.put(
            url,
            data=csv_bytes,
            headers=headers,
            auth=(self.stream_load_config['user'], self.stream_load_config['password'])
        )

        if response.status_code != 200:
            raise Exception(f"Stream Load failed: {response.text}")

        result = response.json()

        if result.get('Status') != 'Success':
            raise Exception(f"Stream Load failed: {result}")

        return result


# 全局实例
excel_handler = ExcelUploadHandler()

