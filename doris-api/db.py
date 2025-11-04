"""
Doris 数据库连接和查询工具
"""
import pymysql
from typing import List, Dict, Any
from config import DORIS_CONFIG


class DorisClient:
    """Doris 数据库客户端"""
    
    def __init__(self):
        self.config = DORIS_CONFIG
    
    def get_connection(self):
        """获取数据库连接"""
        return pymysql.connect(**self.config)
    
    def execute_query(self, sql: str, params: tuple = None) -> List[Dict[str, Any]]:
        """
        执行查询并返回结果
        
        Args:
            sql: SQL 语句
            params: 参数 (可选)
        
        Returns:
            查询结果列表
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(sql, params)
            result = cursor.fetchall()
            return result
        finally:
            conn.close()
    
    def execute_update(self, sql: str, params: tuple = None) -> int:
        """
        执行更新操作 (INSERT/UPDATE/DELETE/CREATE)
        
        Args:
            sql: SQL 语句
            params: 参数 (可选)
        
        Returns:
            影响的行数
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            affected_rows = cursor.execute(sql, params)
            conn.commit()
            return affected_rows
        finally:
            conn.close()
    
    def get_tables(self) -> List[str]:
        """获取所有表名"""
        sql = "SHOW TABLES"
        result = self.execute_query(sql)
        # 结果格式: [{'Tables_in_default': 'table1'}, ...]
        key = f"Tables_in_{self.config['database']}"
        return [row[key] for row in result]
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """获取表结构"""
        sql = f"DESCRIBE {table_name}"
        return self.execute_query(sql)
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return table_name in self.get_tables()

    def _escape_string(self, text: str) -> str:
        """
        转义字符串用于 SQL 查询

        Args:
            text: 原始文本

        Returns:
            转义后的文本 (带引号)
        """
        # 使用 pymysql 的转义功能
        conn = self.get_connection()
        try:
            escaped = conn.escape_string(text)
            return f"'{escaped}'"
        finally:
            conn.close()


# 全局单例
doris_client = DorisClient()

