"""
Doris API Gateway 配置文件
"""

import os

# Doris 连接配置
DORIS_CONFIG = {
    'host': os.getenv('DORIS_HOST', 'localhost'),
    'port': int(os.getenv('DORIS_PORT', '19030')),  # MySQL 协议端口
    'user': 'root',
    'password': '',
    'database': 'test_db',
    'charset': 'utf8mb4'
}

# Doris Stream Load 配置
DORIS_STREAM_LOAD = {
    'host': os.getenv('DORIS_STREAM_LOAD_HOST', 'localhost'),
    'port': int(os.getenv('DORIS_STREAM_LOAD_PORT', '18040')),  # BE HTTP 端口
    'user': 'root',
    'password': ''
}

# 默认 LLM 资源名称
DEFAULT_LLM_RESOURCE = 'default_llm'

# API 配置
API_HOST = '0.0.0.0'
API_PORT = 8000

# 上传文件限制
MAX_UPLOAD_SIZE = 100 * 1024 * 1024  # 100MB

