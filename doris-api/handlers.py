"""
API 请求处理器
"""
from typing import Dict, Any, List, Optional
from db import doris_client
from config import DEFAULT_LLM_RESOURCE


class ActionHandler:
    """统一的 Action 处理器"""
    
    def __init__(self):
        self.db = doris_client
    
    def execute(self, action: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行指定的 action
        
        Args:
            action: 操作类型 (query/sentiment/classify/extract/stats)
            params: 参数字典
        
        Returns:
            执行结果
        """
        handlers = {
            'query': self.handle_query,
            'sentiment': self.handle_sentiment,
            'classify': self.handle_classify,
            'extract': self.handle_extract,
            'stats': self.handle_stats,
            'similarity': self.handle_similarity,
            'translate': self.handle_translate,
            'summarize': self.handle_summarize,
            'mask': self.handle_mask,
            'fixgrammar': self.handle_fixgrammar,
            'generate': self.handle_generate,
            'filter': self.handle_filter
        }
        
        handler = handlers.get(action)
        if not handler:
            raise ValueError(f"Unknown action: {action}")
        
        return handler(params)
    
    def handle_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行普通查询
        
        Params:
            table: 表名
            columns: 列名列表 (可选,默认 *)
            filter: WHERE 条件 (可选)
            limit: 限制行数 (可选,默认 100)
        """
        table = params['table']
        columns = params.get('columns', ['*'])
        filter_clause = params.get('filter', '')
        limit = params.get('limit', 100)
        
        columns_str = ', '.join(columns) if isinstance(columns, list) else columns
        
        sql = f"SELECT {columns_str} FROM {table}"
        if filter_clause:
            sql += f" WHERE {filter_clause}"
        sql += f" LIMIT {limit}"
        
        result = self.db.execute_query(sql)
        
        return {
            'success': True,
            'data': result,
            'count': len(result),
            'sql': sql
        }
    
    def handle_sentiment(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        情感分析
        
        Params:
            table: 表名
            column: 文本列名
            filter: WHERE 条件 (可选)
            limit: 限制行数 (可选,默认 100)
            resource: LLM 资源名 (可选,使用默认)
        """
        table = params['table']
        column = params['column']
        filter_clause = params.get('filter', '')
        limit = params.get('limit', 100)
        resource = params.get('resource', DEFAULT_LLM_RESOURCE)
        
        sql = f"""
        SELECT 
            {column},
            LLM_SENTIMENT('{resource}', {column}) AS sentiment
        FROM {table}
        """
        if filter_clause:
            sql += f" WHERE {filter_clause}"
        sql += f" LIMIT {limit}"
        
        result = self.db.execute_query(sql)
        
        # 统计各情感的数量
        sentiment_counts = {}
        for row in result:
            sentiment = row.get('sentiment', 'unknown')
            sentiment_counts[sentiment] = sentiment_counts.get(sentiment, 0) + 1
        
        return {
            'success': True,
            'data': result,
            'count': len(result),
            'summary': sentiment_counts,
            'sql': sql
        }
    
    def handle_classify(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        文本分类
        
        Params:
            table: 表名
            column: 文本列名
            labels: 分类标签列表
            filter: WHERE 条件 (可选)
            limit: 限制行数 (可选,默认 100)
            resource: LLM 资源名 (可选)
        """
        table = params['table']
        column = params['column']
        labels = params['labels']
        filter_clause = params.get('filter', '')
        limit = params.get('limit', 100)
        resource = params.get('resource', DEFAULT_LLM_RESOURCE)
        
        # 构造标签数组字符串
        labels_str = "['" + "','".join(labels) + "']"
        
        sql = f"""
        SELECT 
            {column},
            LLM_CLASSIFY('{resource}', {column}, {labels_str}) AS category
        FROM {table}
        """
        if filter_clause:
            sql += f" WHERE {filter_clause}"
        sql += f" LIMIT {limit}"
        
        result = self.db.execute_query(sql)
        
        # 统计各分类的数量
        category_counts = {}
        for row in result:
            category = row.get('category', 'unknown')
            category_counts[category] = category_counts.get(category, 0) + 1
        
        return {
            'success': True,
            'data': result,
            'count': len(result),
            'summary': category_counts,
            'sql': sql
        }
    
    def handle_extract(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        信息提取
        
        Params:
            table: 表名
            column: 文本列名
            fields: 要提取的字段列表
            filter: WHERE 条件 (可选)
            limit: 限制行数 (可选,默认 100)
            resource: LLM 资源名 (可选)
        """
        table = params['table']
        column = params['column']
        fields = params['fields']
        filter_clause = params.get('filter', '')
        limit = params.get('limit', 100)
        resource = params.get('resource', DEFAULT_LLM_RESOURCE)
        
        # 构造字段数组字符串
        fields_str = "['" + "','".join(fields) + "']"
        
        sql = f"""
        SELECT 
            {column},
            LLM_EXTRACT('{resource}', {column}, {fields_str}) AS extracted
        FROM {table}
        """
        if filter_clause:
            sql += f" WHERE {filter_clause}"
        sql += f" LIMIT {limit}"
        
        result = self.db.execute_query(sql)
        
        return {
            'success': True,
            'data': result,
            'count': len(result),
            'sql': sql
        }
    
    def handle_stats(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        统计分析
        
        Params:
            table: 表名
            group_by: 分组字段
            metrics: 聚合指标列表 (如 ["SUM(amount)", "COUNT(*)"])
            filter: WHERE 条件 (可选)
        """
        table = params['table']
        group_by = params['group_by']
        metrics = params['metrics']
        filter_clause = params.get('filter', '')
        
        metrics_str = ', '.join(metrics)
        
        sql = f"""
        SELECT 
            {group_by},
            {metrics_str}
        FROM {table}
        """
        if filter_clause:
            sql += f" WHERE {filter_clause}"
        sql += f" GROUP BY {group_by}"
        
        result = self.db.execute_query(sql)
        
        return {
            'success': True,
            'data': result,
            'count': len(result),
            'sql': sql
        }
    
    def handle_similarity(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        语义相似度分析
        
        Params:
            table: 表名
            column1: 第一个文本列
            column2: 第二个文本列
            filter: WHERE 条件 (可选)
            limit: 限制行数 (可选,默认 100)
            resource: LLM 资源名 (可选)
        """
        table = params['table']
        column1 = params['column1']
        column2 = params['column2']
        filter_clause = params.get('filter', '')
        limit = params.get('limit', 100)
        resource = params.get('resource', DEFAULT_LLM_RESOURCE)
        
        sql = f"""
        SELECT 
            {column1},
            {column2},
            LLM_SIMILARITY('{resource}', {column1}, {column2}) AS similarity_score
        FROM {table}
        """
        if filter_clause:
            sql += f" WHERE {filter_clause}"
        sql += f" LIMIT {limit}"
        
        result = self.db.execute_query(sql)
        
        return {
            'success': True,
            'data': result,
            'count': len(result),
            'sql': sql
        }
    
    def handle_translate(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        文本翻译
        
        Params:
            table: 表名
            column: 文本列名
            target_language: 目标语言
            filter: WHERE 条件 (可选)
            limit: 限制行数 (可选,默认 100)
            resource: LLM 资源名 (可选)
        """
        table = params['table']
        column = params['column']
        target_language = params['target_language']
        filter_clause = params.get('filter', '')
        limit = params.get('limit', 100)
        resource = params.get('resource', DEFAULT_LLM_RESOURCE)
        
        sql = f"""
        SELECT 
            {column},
            LLM_TRANSLATE('{resource}', {column}, '{target_language}') AS translated
        FROM {table}
        """
        if filter_clause:
            sql += f" WHERE {filter_clause}"
        sql += f" LIMIT {limit}"
        
        result = self.db.execute_query(sql)
        
        return {
            'success': True,
            'data': result,
            'count': len(result),
            'sql': sql
        }
    
    def handle_summarize(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """文本摘要"""
        table = params['table']
        column = params['column']
        filter_clause = params.get('filter', '')
        limit = params.get('limit', 100)
        resource = params.get('resource', DEFAULT_LLM_RESOURCE)
        
        sql = f"""
        SELECT 
            {column},
            LLM_SUMMARIZE('{resource}', {column}) AS summary
        FROM {table}
        """
        if filter_clause:
            sql += f" WHERE {filter_clause}"
        sql += f" LIMIT {limit}"
        
        result = self.db.execute_query(sql)
        
        return {
            'success': True,
            'data': result,
            'count': len(result),
            'sql': sql
        }
    
    def handle_mask(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """敏感信息脱敏"""
        table = params['table']
        column = params['column']
        labels = params['labels']
        filter_clause = params.get('filter', '')
        limit = params.get('limit', 100)
        resource = params.get('resource', DEFAULT_LLM_RESOURCE)
        
        labels_str = "['" + "','".join(labels) + "']"
        
        sql = f"""
        SELECT 
            {column},
            LLM_MASK('{resource}', {column}, {labels_str}) AS masked
        FROM {table}
        """
        if filter_clause:
            sql += f" WHERE {filter_clause}"
        sql += f" LIMIT {limit}"
        
        result = self.db.execute_query(sql)
        
        return {
            'success': True,
            'data': result,
            'count': len(result),
            'sql': sql
        }
    
    def handle_fixgrammar(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """语法纠错"""
        table = params['table']
        column = params['column']
        filter_clause = params.get('filter', '')
        limit = params.get('limit', 100)
        resource = params.get('resource', DEFAULT_LLM_RESOURCE)
        
        sql = f"""
        SELECT 
            {column},
            LLM_FIXGRAMMAR('{resource}', {column}) AS corrected
        FROM {table}
        """
        if filter_clause:
            sql += f" WHERE {filter_clause}"
        sql += f" LIMIT {limit}"
        
        result = self.db.execute_query(sql)
        
        return {
            'success': True,
            'data': result,
            'count': len(result),
            'sql': sql
        }
    
    def handle_generate(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """内容生成"""
        table = params['table']
        column = params['column']
        filter_clause = params.get('filter', '')
        limit = params.get('limit', 100)
        resource = params.get('resource', DEFAULT_LLM_RESOURCE)
        
        sql = f"""
        SELECT 
            {column},
            LLM_GENERATE('{resource}', {column}) AS generated
        FROM {table}
        """
        if filter_clause:
            sql += f" WHERE {filter_clause}"
        sql += f" LIMIT {limit}"
        
        result = self.db.execute_query(sql)
        
        return {
            'success': True,
            'data': result,
            'count': len(result),
            'sql': sql
        }
    
    def handle_filter(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """布尔过滤"""
        table = params['table']
        column = params['column']
        condition = params['condition']
        limit = params.get('limit', 100)
        resource = params.get('resource', DEFAULT_LLM_RESOURCE)
        
        sql = f"""
        SELECT 
            {column},
            LLM_FILTER('{resource}', CONCAT('{condition}', {column})) AS is_valid
        FROM {table}
        WHERE LLM_FILTER('{resource}', CONCAT('{condition}', {column})) = 1
        LIMIT {limit}
        """
        
        result = self.db.execute_query(sql)
        
        return {
            'success': True,
            'data': result,
            'count': len(result),
            'sql': sql
        }


# 全局处理器实例
action_handler = ActionHandler()

