"""
外部数据源同步处理器
"""
import pymysql
import pandas as pd
import json
import os
import hashlib
from typing import Dict, Any, List, Optional
from datetime import datetime
from cryptography.fernet import Fernet
from db import doris_client
from upload_handler import excel_handler


class DataSourceHandler:
    """外部数据源管理和同步处理器"""
    
    def __init__(self):
        self.db = doris_client
        # 加密密钥 - 从环境变量获取
        key = os.getenv('ENCRYPTION_KEY')
        if key:
            # 使用环境变量中的密钥
            self.cipher = Fernet(key.encode() if isinstance(key, str) else key)
        else:
            # 使用固定的默认密钥（仅用于开发环境）
            default_key = b'dITsw-d5mJGd4qrPln29AldqAy8GCb4lMvvZvQGRBQU='
            self.cipher = Fernet(default_key)
        self._tables_initialized = False

    def init_tables(self):
        """初始化系统表（在数据库就绪后调用）"""
        if not self._tables_initialized:
            self._ensure_system_tables()
            self._tables_initialized = True

    def _ensure_system_tables(self):
        """确保系统表存在"""
        # 数据源配置表
        sql_datasources = """
        CREATE TABLE IF NOT EXISTS `_sys_datasources` (
            `id` VARCHAR(64),
            `name` VARCHAR(200),
            `host` VARCHAR(200),
            `port` INT,
            `user` VARCHAR(100),
            `password_encrypted` VARCHAR(500),
            `database_name` VARCHAR(200),
            `created_at` DATETIME,
            `updated_at` DATETIME
        )
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """
        
        # 同步任务表
        sql_sync_tasks = """
        CREATE TABLE IF NOT EXISTS `_sys_sync_tasks` (
            `id` VARCHAR(64),
            `datasource_id` VARCHAR(64),
            `source_table` VARCHAR(200),
            `target_table` VARCHAR(200),
            `schedule_type` VARCHAR(50),
            `schedule_value` VARCHAR(100),
            `last_sync_at` DATETIME,
            `next_sync_at` DATETIME,
            `status` VARCHAR(50),
            `created_at` DATETIME
        )
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """
        
        # 表元数据表
        sql_metadata = """
        CREATE TABLE IF NOT EXISTS `_sys_table_metadata` (
            `table_name` VARCHAR(200),
            `description` TEXT,
            `columns_info` TEXT,
            `sample_queries` TEXT,
            `analyzed_at` DATETIME,
            `source_type` VARCHAR(50)
        )
        DUPLICATE KEY(`table_name`)
        DISTRIBUTED BY HASH(`table_name`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """
        
        try:
            self.db.execute_update(sql_datasources)
            self.db.execute_update(sql_sync_tasks)
            self.db.execute_update(sql_metadata)
        except Exception as e:
            print(f"Warning: Could not create system tables: {e}")
    
    def _encrypt_password(self, password: str) -> str:
        """加密密码"""
        return self.cipher.encrypt(password.encode()).decode()
    
    def _decrypt_password(self, encrypted: str) -> str:
        """解密密码"""
        return self.cipher.decrypt(encrypted.encode()).decode()
    
    def test_connection(self, host: str, port: int, user: str, 
                       password: str, database: str = None) -> Dict[str, Any]:
        """测试数据库连接"""
        try:
            conn_params = {
                'host': host,
                'port': port,
                'user': user,
                'password': password,
                'connect_timeout': 10
            }
            if database:
                conn_params['database'] = database
                
            conn = pymysql.connect(**conn_params)
            cursor = conn.cursor()
            
            # 获取数据库列表
            cursor.execute("SHOW DATABASES")
            databases = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            return {
                'success': True,
                'message': '连接成功',
                'databases': databases
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'连接失败: {str(e)}',
                'databases': []
            }
    
    def get_remote_tables(self, host: str, port: int, user: str,
                         password: str, database: str) -> Dict[str, Any]:
        """获取远程数据库的表列表"""
        try:
            conn = pymysql.connect(
                host=host, port=port, user=user,
                password=password, database=database,
                connect_timeout=10
            )
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            
            # 获取表列表和基本信息
            cursor.execute("""
                SELECT 
                    TABLE_NAME as name,
                    TABLE_ROWS as row_count,
                    TABLE_COMMENT as comment
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
            """, (database,))
            tables = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            return {
                'success': True,
                'tables': tables,
                'count': len(tables)
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'tables': []
            }

    def save_datasource(self, name: str, host: str, port: int,
                       user: str, password: str, database: str) -> Dict[str, Any]:
        """保存数据源配置"""
        import uuid

        ds_id = str(uuid.uuid4())[:8]
        encrypted_pwd = self._encrypt_password(password)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        sql = """
        INSERT INTO `_sys_datasources`
        (`id`, `name`, `host`, `port`, `user`, `password_encrypted`,
         `database_name`, `created_at`, `updated_at`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.db.execute_update(sql, (
            ds_id, name, host, port, user, encrypted_pwd,
            database, now, now
        ))

        return {
            'success': True,
            'id': ds_id,
            'message': f'数据源 "{name}" 保存成功'
        }

    def list_datasources(self) -> List[Dict[str, Any]]:
        """获取所有数据源"""
        sql = """
        SELECT id, name, host, port, user, database_name, created_at
        FROM `_sys_datasources`
        ORDER BY created_at DESC
        """
        return self.db.execute_query(sql)

    def get_datasource(self, ds_id: str) -> Optional[Dict[str, Any]]:
        """获取单个数据源配置（包含解密密码）"""
        sql = "SELECT * FROM `_sys_datasources` WHERE id = %s"
        results = self.db.execute_query(sql, (ds_id,))
        if results:
            ds = results[0]
            ds['password'] = self._decrypt_password(ds['password_encrypted'])
            del ds['password_encrypted']
            return ds
        return None

    def delete_datasource(self, ds_id: str) -> Dict[str, Any]:
        """删除数据源"""
        sql = "DELETE FROM `_sys_datasources` WHERE id = %s"
        self.db.execute_update(sql, (ds_id,))
        return {'success': True, 'message': '数据源已删除'}

    def sync_table(self, ds_id: str, source_table: str,
                   target_table: str = None) -> Dict[str, Any]:
        """同步单个表"""
        ds = self.get_datasource(ds_id)
        if not ds:
            return {'success': False, 'error': '数据源不存在'}

        if not target_table:
            target_table = source_table

        try:
            # 连接远程数据库
            conn = pymysql.connect(
                host=ds['host'], port=ds['port'],
                user=ds['user'], password=ds['password'],
                database=ds['database_name'],
                connect_timeout=30
            )

            # 读取数据
            df = pd.read_sql(f"SELECT * FROM `{source_table}`", conn)
            conn.close()

            if df.empty:
                return {
                    'success': True,
                    'message': '表为空，无数据同步',
                    'rows_synced': 0
                }

            # 清理列名
            df.columns = [col.replace(' ', '_').replace('-', '_') for col in df.columns]

            # 检查目标表是否存在
            table_exists = self.db.table_exists(target_table)

            if not table_exists:
                # 自动推断列类型并创建表
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

                excel_handler.create_table(target_table, column_types)

            # 使用 Stream Load 导入
            result = excel_handler.stream_load(df, target_table)

            return {
                'success': True,
                'source_table': source_table,
                'target_table': target_table,
                'rows_synced': len(df),
                'table_created': not table_exists,
                'stream_load_result': result
            }

        except Exception as e:
            import traceback
            return {
                'success': False,
                'error': str(e),
                'traceback': traceback.format_exc()
            }

    def sync_multiple_tables(self, ds_id: str,
                            tables: List[Dict[str, str]]) -> Dict[str, Any]:
        """同步多个表"""
        print(f"📦 开始批量同步 {len(tables)} 张表, ds_id={ds_id}")
        print(f"📋 tables: {tables}")

        results = []
        success_count = 0
        fail_count = 0

        for table_config in tables:
            source = table_config.get('source_table')
            target = table_config.get('target_table', source)
            print(f"🔄 同步表: {source} -> {target}")

            result = self.sync_table(ds_id, source, target)
            print(f"📊 同步结果: {result}")

            results.append({
                'source_table': source,
                'target_table': target,
                **result
            })

            if result.get('success'):
                success_count += 1
            else:
                fail_count += 1

        print(f"✅ 批量同步完成: 成功={success_count}, 失败={fail_count}")
        print(f"🔍 详细结果: {json.dumps(results, indent=2, default=str)}")
        
        response = {
            'success': fail_count == 0,
            'total': len(tables),
            'success_count': success_count,
            'fail_count': fail_count,
            'results': results
        }

        if fail_count > 0:
            # 提取第一个失败的错误信息作为主要错误
            failed_results = [r for r in results if not r.get('success')]
            first_error = failed_results[0].get('error', 'Unknown error') if failed_results else 'Unknown error'
            response['error'] = f"同步完成，但在 {fail_count} 张表中发生错误: {first_error}"
            print(f"❌ 设置顶层错误: {response['error']}")
            
        return response

    def save_sync_task(self, ds_id: str, source_table: str,
                       target_table: str, schedule_type: str) -> Dict[str, Any]:
        """保存同步任务配置"""
        import uuid

        task_id = str(uuid.uuid4())[:8]
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 计算下次同步时间
        next_sync = self._calculate_next_sync(schedule_type)

        sql = """
        INSERT INTO `_sys_sync_tasks`
        (`id`, `datasource_id`, `source_table`, `target_table`,
         `schedule_type`, `schedule_value`, `last_sync_at`, `next_sync_at`,
         `status`, `created_at`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.db.execute_update(sql, (
            task_id, ds_id, source_table, target_table or source_table,
            schedule_type, '', now, next_sync, 'active', now
        ))

        return {
            'success': True,
            'task_id': task_id,
            'next_sync_at': next_sync
        }

    def _calculate_next_sync(self, schedule_type: str) -> str:
        """计算下次同步时间"""
        from datetime import timedelta

        now = datetime.now()
        if schedule_type == 'hourly':
            next_time = now + timedelta(hours=1)
        elif schedule_type == 'daily':
            next_time = now + timedelta(days=1)
        elif schedule_type == 'weekly':
            next_time = now + timedelta(weeks=1)
        else:
            next_time = now + timedelta(days=1)

        return next_time.strftime('%Y-%m-%d %H:%M:%S')

    def list_sync_tasks(self) -> List[Dict[str, Any]]:
        """获取所有同步任务"""
        sql = """
        SELECT t.*, d.name as datasource_name
        FROM `_sys_sync_tasks` t
        LEFT JOIN `_sys_datasources` d ON t.datasource_id = d.id
        ORDER BY t.created_at DESC
        """
        return self.db.execute_query(sql)

    def delete_sync_task(self, task_id: str) -> Dict[str, Any]:
        """删除同步任务"""
        sql = "DELETE FROM `_sys_sync_tasks` WHERE id = %s"
        self.db.execute_update(sql, (task_id,))
        return {'success': True, 'message': '同步任务已删除'}

    def get_pending_tasks(self) -> List[Dict[str, Any]]:
        """获取待执行的同步任务"""
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = """
        SELECT * FROM `_sys_sync_tasks`
        WHERE status = 'active' AND next_sync_at <= %s
        """
        return self.db.execute_query(sql, (now,))

    def execute_scheduled_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """执行定时任务"""
        result = self.sync_table(
            ds_id=task['datasource_id'],
            source_table=task['source_table'],
            target_table=task['target_table']
        )

        # 更新任务状态
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        next_sync = self._calculate_next_sync(task['schedule_type'])

        sql = """
        UPDATE `_sys_sync_tasks`
        SET last_sync_at = %s, next_sync_at = %s
        WHERE id = %s
        """
        self.db.execute_update(sql, (now, next_sync, task['id']))

        return result


# 全局实例
datasource_handler = DataSourceHandler()


# ============ 定时调度器 ============

class SyncScheduler:
    """同步任务调度器"""

    def __init__(self, handler: DataSourceHandler):
        self.handler = handler
        self.scheduler = None

    def start(self):
        """启动调度器"""
        try:
            from apscheduler.schedulers.background import BackgroundScheduler

            self.scheduler = BackgroundScheduler()
            # 每分钟检查一次待执行的任务
            self.scheduler.add_job(
                self._check_and_execute_tasks,
                'interval',
                minutes=1,
                id='sync_checker'
            )
            self.scheduler.start()
            print("✅ 同步调度器已启动")
        except Exception as e:
            print(f"⚠️ 同步调度器启动失败: {e}")

    def stop(self):
        """停止调度器"""
        if self.scheduler:
            self.scheduler.shutdown()
            print("🛑 同步调度器已停止")

    def _check_and_execute_tasks(self):
        """检查并执行待同步任务"""
        try:
            tasks = self.handler.get_pending_tasks()
            for task in tasks:
                print(f"⏰ 执行定时同步: {task['source_table']} -> {task['target_table']}")
                result = self.handler.execute_scheduled_task(task)
                if result.get('success'):
                    print(f"✅ 同步成功: {result.get('rows_synced', 0)} 行")
                else:
                    print(f"❌ 同步失败: {result.get('error')}")
        except Exception as e:
            print(f"❌ 任务检查失败: {e}")


# 全局调度器实例
sync_scheduler = SyncScheduler(datasource_handler)

