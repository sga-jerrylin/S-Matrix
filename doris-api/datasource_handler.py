"""
外部数据源同步处理器
"""
import pymysql
import pandas as pd
import json
import os
import hashlib
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
from cryptography.fernet import Fernet
from config import DB_CONNECT_TIMEOUT, DB_READ_TIMEOUT, DB_WRITE_TIMEOUT
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
        # 数据源配置表 - 使用 UNIQUE KEY 以支持 UPDATE/DELETE
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
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """

        # 同步任务表 - 使用 UNIQUE KEY 以支持 UPDATE/DELETE
        sql_sync_tasks = """
        CREATE TABLE IF NOT EXISTS `_sys_sync_tasks` (
            `id` VARCHAR(64),
            `datasource_id` VARCHAR(64),
            `source_table` VARCHAR(200),
            `target_table` VARCHAR(200),
            `schedule_type` VARCHAR(50),
            `schedule_minute` INT DEFAULT "0",
            `schedule_hour` INT DEFAULT "0",
            `schedule_day_of_week` INT DEFAULT "1",
            `schedule_day_of_month` INT DEFAULT "1",
            `schedule_value` VARCHAR(100),
            `last_sync_at` DATETIME,
            `next_sync_at` DATETIME,
            `status` VARCHAR(50),
            `enabled_for_ai` TINYINT DEFAULT "1",
            `created_at` DATETIME
        )
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """

        # 表元数据表 - 使用 UNIQUE KEY 以支持 UPDATE/DELETE
        sql_metadata = """
        CREATE TABLE IF NOT EXISTS `_sys_table_metadata` (
            `table_name` VARCHAR(200),
            `description` TEXT,
            `columns_info` TEXT,
            `sample_queries` TEXT,
            `analyzed_at` DATETIME,
            `source_type` VARCHAR(50)
        )
        UNIQUE KEY(`table_name`)
        DISTRIBUTED BY HASH(`table_name`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """

        sql_table_registry = """
        CREATE TABLE IF NOT EXISTS `_sys_table_registry` (
            `table_name` VARCHAR(200),
            `display_name` VARCHAR(200),
            `description` TEXT,
            `source_type` VARCHAR(50),
            `created_at` DATETIME,
            `updated_at` DATETIME
        )
        UNIQUE KEY(`table_name`)
        DISTRIBUTED BY HASH(`table_name`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """
        
        import time
        max_retries = 10
        for attempt in range(max_retries):
            try:
                self.db.execute_update(sql_datasources)
                self.db.execute_update(sql_sync_tasks)
                self.db.execute_update(sql_metadata)
                self.db.execute_update(sql_table_registry)
                print("✅ 系统表创建成功")
                return
            except Exception as e:
                error_msg = str(e)
                if "available backend num is 0" in error_msg and attempt < max_retries - 1:
                    print(f"⏳ BE 尚未就绪，等待重试... ({attempt + 1}/{max_retries})")
                    time.sleep(5)
                else:
                    print(f"Warning: Could not create system tables: {e}")

    def ensure_table_registry(self, table_name: str, source_type: str,
                              display_name: Optional[str] = None,
                              description: Optional[str] = None) -> Dict[str, Any]:
        """确保表注册存在 (同步)"""
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        exists_sql = "SELECT table_name FROM `_sys_table_registry` WHERE table_name = %s LIMIT 1"
        exists = self.db.execute_query(exists_sql, (table_name,))

        if exists:
            update_sql = """
            UPDATE `_sys_table_registry`
            SET source_type = COALESCE(%s, source_type),
                display_name = COALESCE(%s, display_name),
                description = COALESCE(%s, description),
                updated_at = %s
            WHERE table_name = %s
            """
            self.db.execute_update(update_sql, (source_type, display_name, description, now, table_name))
            return {'success': True, 'message': '表注册已更新', 'table_name': table_name}

        insert_sql = """
        INSERT INTO `_sys_table_registry`
        (`table_name`, `display_name`, `description`, `source_type`, `created_at`, `updated_at`)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        self.db.execute_update(insert_sql, (
            table_name,
            display_name if display_name is not None else '',
            description if description is not None else '',
            source_type,
            now,
            now
        ))
        return {'success': True, 'message': '表注册已创建', 'table_name': table_name}
    
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
                'connect_timeout': 30,
                'read_timeout': 30
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
                connect_timeout=30,
                read_timeout=60
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

    def preview_remote_table(self, host: str, port: int, user: str,
                              password: str, database: str, table_name: str,
                              limit: int = 100) -> Dict[str, Any]:
        """预览远程表的结构和数据"""
        try:
            conn = pymysql.connect(
                host=host, port=port, user=user,
                password=password, database=database,
                connect_timeout=10
            )
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            # 获取表结构
            cursor.execute(f"""
                SELECT
                    COLUMN_NAME as name,
                    DATA_TYPE as type,
                    COLUMN_TYPE as full_type,
                    IS_NULLABLE as nullable,
                    COLUMN_COMMENT as comment
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (database, table_name))
            columns = cursor.fetchall()

            # 获取前100行数据
            cursor.execute(f"SELECT * FROM `{table_name}` LIMIT %s", (limit,))
            data = cursor.fetchall()

            # 获取总行数
            cursor.execute(f"SELECT COUNT(*) as total FROM `{table_name}`")
            total = cursor.fetchone()['total']

            cursor.close()
            conn.close()

            return {
                'success': True,
                'table_name': table_name,
                'columns': columns,
                'data': data,
                'total_rows': total,
                'preview_rows': len(data)
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
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
            # 连接远程数据库 (使用 SSCursor 实现流式读取)
            conn = pymysql.connect(
                host=ds['host'], port=ds['port'],
                user=ds['user'], password=ds['password'],
                database=ds['database_name'],
                connect_timeout=60,  # 增加连接超时
                cursorclass=pymysql.cursors.SSCursor  # 关键：使用服务端游标
            )

            # 使用 chunksize 分批读取
            chunk_size = 10000  # 减小分批大小，降低内存压力
            total_rows_synced = 0
            table_created_in_this_process = False
            last_stream_load_result = None

            try:
                cursor = conn.cursor()
                source_table_safe = f"`{source_table}`"
                cursor.execute(f"SELECT * FROM {source_table_safe}")
                
                # 获取列名
                columns = [col[0] for col in cursor.description]
                
                batch_count = 0
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    
                    batch_count += 1
                    # 转换为 DataFrame 以复用现有逻辑
                    df = pd.DataFrame(rows, columns=columns)

                    # 清理列名
                    df.columns = [col.replace(' ', '_').replace('-', '_') for col in df.columns]

                    # 仅在第一批次检查和创建表
                    if batch_count == 1:
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
                            table_created_in_this_process = True
                        else:
                            safe_target = self.db.validate_identifier(target_table)
                            try:
                                self.db.execute_update(f"TRUNCATE TABLE {safe_target}")
                            except Exception:
                                self.db.execute_update(f"DELETE FROM {safe_target} WHERE 1=1")

                    # 使用 Stream Load 导入当前批次
                    print(f"🔄 Importing batch {batch_count} ({len(df)} rows) into {target_table}...")
                    last_stream_load_result = excel_handler.stream_load(df, target_table)
                    total_rows_synced += len(df)
            
            finally:
                conn.close()
            
            if total_rows_synced == 0:
                 return {
                    'success': True,
                    'message': '表为空，无数据同步',
                    'rows_synced': 0
                }

            return {
                'success': True,
                'source_table': source_table,
                'target_table': target_table,
                'rows_synced': total_rows_synced,
                'table_created': table_created_in_this_process,
                'stream_load_result': last_stream_load_result
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
                       target_table: str, schedule_type: str,
                       schedule_minute: int = 0, schedule_hour: int = 0,
                       schedule_day_of_week: int = 1, schedule_day_of_month: int = 1,
                       enabled_for_ai: bool = True) -> Dict[str, Any]:
        """
        保存同步任务配置（增强版）

        Args:
            ds_id: 数据源ID
            source_table: 源表名
            target_table: 目标表名
            schedule_type: 调度类型 (hourly/daily/weekly/monthly)
            schedule_minute: 分钟 (0-59)
            schedule_hour: 小时 (0-23)
            schedule_day_of_week: 周几 (1-7, 1=周一)
            schedule_day_of_month: 日期 (1-31)
            enabled_for_ai: 是否启用AI分析
        """
        import uuid

        task_id = str(uuid.uuid4())[:8]
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 计算下次同步时间
        next_sync = self._calculate_next_sync_detailed(
            schedule_type, schedule_minute, schedule_hour,
            schedule_day_of_week, schedule_day_of_month
        )

        sql = """
        INSERT INTO `_sys_sync_tasks`
        (`id`, `datasource_id`, `source_table`, `target_table`,
         `schedule_type`, `schedule_minute`, `schedule_hour`,
         `schedule_day_of_week`, `schedule_day_of_month`,
         `schedule_value`, `last_sync_at`, `next_sync_at`,
         `status`, `enabled_for_ai`, `created_at`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        self.db.execute_update(sql, (
            task_id, ds_id, source_table, target_table or source_table,
            schedule_type, schedule_minute, schedule_hour,
            schedule_day_of_week, schedule_day_of_month,
            '', now, next_sync, 'active', 1 if enabled_for_ai else 0, now
        ))

        return {
            'success': True,
            'task_id': task_id,
            'next_sync_at': next_sync,
            'schedule_description': self._get_schedule_description(
                schedule_type, schedule_minute, schedule_hour,
                schedule_day_of_week, schedule_day_of_month
            )
        }

    def update_sync_task(self, task_id: str, schedule_type: str = None,
                         schedule_minute: int = None, schedule_hour: int = None,
                         schedule_day_of_week: int = None, schedule_day_of_month: int = None,
                         enabled_for_ai: bool = None) -> Dict[str, Any]:
        """更新同步任务配置"""
        updates = []
        params = []

        if schedule_type is not None:
            updates.append("schedule_type = %s")
            params.append(schedule_type)
        if schedule_minute is not None:
            updates.append("schedule_minute = %s")
            params.append(schedule_minute)
        if schedule_hour is not None:
            updates.append("schedule_hour = %s")
            params.append(schedule_hour)
        if schedule_day_of_week is not None:
            updates.append("schedule_day_of_week = %s")
            params.append(schedule_day_of_week)
        if schedule_day_of_month is not None:
            updates.append("schedule_day_of_month = %s")
            params.append(schedule_day_of_month)
        if enabled_for_ai is not None:
            updates.append("enabled_for_ai = %s")
            params.append(1 if enabled_for_ai else 0)

        if not updates:
            return {'success': False, 'error': '没有要更新的字段'}

        params.append(task_id)
        sql = f"UPDATE `_sys_sync_tasks` SET {', '.join(updates)} WHERE id = %s"
        self.db.execute_update(sql, tuple(params))

        return {'success': True, 'message': '任务已更新'}

    def toggle_ai_enabled(self, task_id: str, enabled: bool) -> Dict[str, Any]:
        """切换表的AI分析启用状态"""
        sql = "UPDATE `_sys_sync_tasks` SET enabled_for_ai = %s WHERE id = %s"
        self.db.execute_update(sql, (1 if enabled else 0, task_id))
        return {
            'success': True,
            'enabled_for_ai': enabled,
            'message': f'AI分析已{"启用" if enabled else "禁用"}'
        }

    def get_ai_enabled_tables(self) -> List[str]:
        """获取所有启用AI分析的表名"""
        sql = "SELECT DISTINCT target_table FROM `_sys_sync_tasks` WHERE enabled_for_ai = 1"
        results = self.db.execute_query(sql)
        return [r['target_table'] for r in results]

    def _get_schedule_description(self, schedule_type: str, minute: int, hour: int,
                                   day_of_week: int, day_of_month: int) -> str:
        """生成调度描述"""
        weekdays = ['', '周一', '周二', '周三', '周四', '周五', '周六', '周日']
        time_str = f"{hour:02d}:{minute:02d}"

        if schedule_type == 'hourly':
            return f"每小时第{minute}分钟"
        elif schedule_type == 'daily':
            return f"每天 {time_str}"
        elif schedule_type == 'weekly':
            return f"每{weekdays[day_of_week]} {time_str}"
        elif schedule_type == 'monthly':
            return f"每月{day_of_month}号 {time_str}"
        return schedule_type

    def _calculate_next_sync_detailed(self, schedule_type: str, minute: int, hour: int,
                                       day_of_week: int, day_of_month: int) -> str:
        """计算下次同步时间（详细版）"""
        from datetime import timedelta

        now = datetime.now()

        if schedule_type == 'hourly':
            # 下一个小时的第N分钟
            next_time = now.replace(minute=minute, second=0, microsecond=0)
            if next_time <= now:
                next_time += timedelta(hours=1)

        elif schedule_type == 'daily':
            # 明天的指定时间
            next_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if next_time <= now:
                next_time += timedelta(days=1)

        elif schedule_type == 'weekly':
            # 下一个指定周几的指定时间
            next_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            days_ahead = day_of_week - now.isoweekday()
            if days_ahead < 0 or (days_ahead == 0 and next_time <= now):
                days_ahead += 7
            next_time += timedelta(days=days_ahead)

        elif schedule_type == 'monthly':
            # 下个月的指定日期时间
            next_time = now.replace(day=min(day_of_month, 28), hour=hour,
                                     minute=minute, second=0, microsecond=0)
            if next_time <= now:
                # 移到下个月
                if now.month == 12:
                    next_time = next_time.replace(year=now.year + 1, month=1)
                else:
                    next_time = next_time.replace(month=now.month + 1)
        else:
            next_time = now + timedelta(days=1)

        return next_time.strftime('%Y-%m-%d %H:%M:%S')

    def _calculate_next_sync(self, schedule_type: str) -> str:
        """计算下次同步时间（简化版，保持向后兼容）"""
        return self._calculate_next_sync_detailed(schedule_type, 0, 0, 1, 1)

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

    # ============ 异步包装方法 (for FastAPI async endpoints) ============

    async def test_connection(self, host: str, port: int, user: str,
                              password: str, database: str = None) -> Dict[str, Any]:
        """测试数据库连接 (异步)"""
        return await asyncio.to_thread(
            self._test_connection_sync, host, port, user, password, database
        )

    def _test_connection_sync(self, host: str, port: int, user: str,
                              password: str, database: str = None) -> Dict[str, Any]:
        """测试数据库连接 (同步)"""
        try:
            conn_params = {
                'host': host,
                'port': port,
                'user': user,
                'password': password,
                'connect_timeout': 30,
                'read_timeout': 30
            }
            if database:
                conn_params['database'] = database

            conn = pymysql.connect(**conn_params)
            cursor = conn.cursor()

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

    async def save_datasource(self, name: str, host: str, port: int,
                              user: str, password: str, database: str) -> Dict[str, Any]:
        """保存数据源配置 (异步)"""
        return await asyncio.to_thread(
            self._save_datasource_sync, name, host, port, user, password, database
        )

    async def list_datasources(self) -> List[Dict[str, Any]]:
        """获取所有数据源 (异步)"""
        return await asyncio.to_thread(self._list_datasources_sync)

    async def get_datasource(self, ds_id: str) -> Optional[Dict[str, Any]]:
        """获取单个数据源配置 (异步)"""
        return await asyncio.to_thread(self._get_datasource_sync, ds_id)

    async def delete_datasource(self, ds_id: str) -> Dict[str, Any]:
        """删除数据源 (异步)"""
        return await asyncio.to_thread(self._delete_datasource_sync, ds_id)

    async def get_remote_tables(self, host: str, port: int, user: str,
                                password: str, database: str) -> Dict[str, Any]:
        """获取远程数据库的表列表 (异步)"""
        return await asyncio.to_thread(
            self._get_remote_tables_sync, host, port, user, password, database
        )

    async def sync_table(self, ds_id: str, source_table: str,
                         target_table: str = None) -> Dict[str, Any]:
        """同步单个表 (异步)"""
        return await asyncio.to_thread(self._sync_table_sync, ds_id, source_table, target_table)

    async def sync_multiple_tables(self, ds_id: str,
                                   tables: List[Dict[str, str]]) -> Dict[str, Any]:
        """同步多个表 (异步)"""
        return await asyncio.to_thread(self._sync_multiple_tables_sync, ds_id, tables)

    async def preview_remote_table(self, host: str, port: int, user: str,
                                   password: str, database: str, table_name: str,
                                   limit: int = 100) -> Dict[str, Any]:
        """预览远程表的结构和数据 (异步)"""
        return await asyncio.to_thread(
            self._preview_remote_table_sync, host, port, user, password, database, table_name, limit
        )

    async def save_sync_task(self, ds_id: str, source_table: str,
                             target_table: str, schedule_type: str,
                             schedule_minute: int = 0, schedule_hour: int = 0,
                             schedule_day_of_week: int = 1, schedule_day_of_month: int = 1,
                             enabled_for_ai: bool = True) -> Dict[str, Any]:
        """保存同步任务配置 (异步)"""
        return await asyncio.to_thread(
            self._save_sync_task_sync, ds_id, source_table, target_table, schedule_type,
            schedule_minute, schedule_hour, schedule_day_of_week, schedule_day_of_month, enabled_for_ai
        )

    async def update_sync_task(self, task_id: str, schedule_type: str,
                               schedule_minute: int = 0, schedule_hour: int = 0,
                               schedule_day_of_week: int = 1, schedule_day_of_month: int = 1,
                               enabled_for_ai: bool = True) -> Dict[str, Any]:
        """更新同步任务配置 (异步)"""
        return await asyncio.to_thread(
            self._update_sync_task_sync, task_id, schedule_type,
            schedule_minute, schedule_hour, schedule_day_of_week, schedule_day_of_month, enabled_for_ai
        )

    async def toggle_ai_enabled(self, task_id: str, enabled: bool) -> Dict[str, Any]:
        """切换任务的AI启用状态 (异步)"""
        return await asyncio.to_thread(self._toggle_ai_enabled_sync, task_id, enabled)

    async def list_sync_tasks(self) -> List[Dict[str, Any]]:
        """获取所有同步任务 (异步)"""
        return await asyncio.to_thread(self._list_sync_tasks_sync)

    async def get_ai_enabled_tables(self) -> List[str]:
        """获取所有启用AI的表名 (异步)"""
        return await asyncio.to_thread(self._get_ai_enabled_tables_sync)

    async def delete_sync_task(self, task_id: str) -> Dict[str, Any]:
        """删除同步任务 (异步)"""
        return await asyncio.to_thread(self._delete_sync_task_sync, task_id)

    async def list_table_registry(self) -> List[Dict[str, Any]]:
        """获取表注册列表 (异步)"""
        return await asyncio.to_thread(self._list_table_registry_sync)

    async def update_table_registry(self, table_name: str, display_name: str = None,
                                    description: str = None) -> Dict[str, Any]:
        """更新表注册信息 (异步)"""
        return await asyncio.to_thread(
            self._update_table_registry_sync, table_name, display_name, description
        )

    async def ensure_table_registry_async(self, table_name: str, source_type: str) -> Dict[str, Any]:
        """确保表注册存在 (异步)"""
        return await asyncio.to_thread(self.ensure_table_registry, table_name, source_type)

    # ============ 同步方法别名 (供异步方法调用) ============
    # 这些别名让异步包装器可以调用原有的同步方法

    def _save_datasource_sync(self, name, host, port, user, password, database):
        """保存数据源配置 (同步)"""
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
        return {'success': True, 'id': ds_id, 'message': f'数据源 "{name}" 保存成功'}

    def _list_datasources_sync(self):
        """获取所有数据源 (同步)"""
        sql = """
        SELECT id, name, host, port, user, database_name, created_at
        FROM `_sys_datasources`
        ORDER BY created_at DESC
        """
        return self.db.execute_query(sql)

    def _get_datasource_sync(self, ds_id):
        """获取单个数据源配置 (同步)"""
        sql = "SELECT * FROM `_sys_datasources` WHERE id = %s"
        results = self.db.execute_query(sql, (ds_id,))
        if results:
            ds = results[0]
            ds['password'] = self._decrypt_password(ds['password_encrypted'])
            del ds['password_encrypted']
            return ds
        return None

    def _delete_datasource_sync(self, ds_id):
        """删除数据源 (同步)"""
        sql = "DELETE FROM `_sys_datasources` WHERE id = %s"
        self.db.execute_update(sql, (ds_id,))
        return {'success': True, 'message': '数据源已删除'}

    def _get_remote_tables_sync(self, host, port, user, password, database):
        """获取远程数据库的表列表 (同步)"""
        try:
            conn = pymysql.connect(
                host=host, port=port, user=user,
                password=password, database=database,
                connect_timeout=30,
                read_timeout=60
            )
            cursor = conn.cursor(pymysql.cursors.DictCursor)
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
            return {'success': True, 'tables': tables, 'count': len(tables)}
        except Exception as e:
            return {'success': False, 'error': str(e), 'tables': []}

    def _sync_table_sync(self, ds_id, source_table, target_table=None):
        """同步单个表 (同步) - 调用原始 sync_table_original"""
        # 获取数据源
        ds = self._get_datasource_sync(ds_id)
        if not ds:
            return {'success': False, 'error': '数据源不存在'}

        if not target_table:
            target_table = source_table

        try:
            conn = pymysql.connect(
                host=ds['host'], port=ds['port'],
                user=ds['user'], password=ds['password'],
                database=ds['database_name'],
                connect_timeout=DB_CONNECT_TIMEOUT,
                read_timeout=DB_READ_TIMEOUT,
                write_timeout=DB_WRITE_TIMEOUT
            )
            df = pd.read_sql(f"SELECT * FROM `{source_table}`", conn)
            conn.close()

            if df.empty:
                return {'success': True, 'message': '表为空', 'rows_synced': 0}

            df.columns = [col.replace(' ', '_').replace('-', '_') for col in df.columns]
            table_exists = self.db.table_exists(target_table)

            if not table_exists:
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
            else:
                safe_target = self.db.validate_identifier(target_table)
                try:
                    self.db.execute_update(f"TRUNCATE TABLE {safe_target}")
                except Exception:
                    self.db.execute_update(f"DELETE FROM {safe_target} WHERE 1=1")

            result = excel_handler.stream_load(df, target_table)
            return {
                'success': True, 'source_table': source_table, 'target_table': target_table,
                'rows_synced': len(df), 'table_created': not table_exists, 'stream_load_result': result
            }
        except Exception as e:
            import traceback
            return {'success': False, 'error': str(e), 'traceback': traceback.format_exc()}

    def _sync_multiple_tables_sync(self, ds_id, tables):
        """同步多个表 (同步)"""
        results = []
        success_count = 0
        fail_count = 0
        for table_config in tables:
            source = table_config.get('source_table')
            target = table_config.get('target_table', source)
            result = self._sync_table_sync(ds_id, source, target)
            results.append({'source_table': source, 'target_table': target, **result})
            if result.get('success'):
                success_count += 1
            else:
                fail_count += 1
        return {'success': fail_count == 0, 'total': len(tables), 'success_count': success_count,
                'fail_count': fail_count, 'results': results}

    def _preview_remote_table_sync(self, host, port, user, password, database, table_name, limit=100):
        """预览远程表 (同步)"""
        try:
            conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database,
                                   connect_timeout=30, read_timeout=60)
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute("""SELECT COLUMN_NAME as name, DATA_TYPE as type FROM information_schema.COLUMNS
                              WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION""", (database, table_name))
            columns = cursor.fetchall()
            safe_table_name = self.db.validate_identifier(table_name)
            cursor.execute(f"SELECT * FROM {safe_table_name} LIMIT %s", (limit,))
            data = cursor.fetchall()
            cursor.execute(f"SELECT COUNT(*) as total FROM {safe_table_name}")
            total = cursor.fetchone()['total']
            cursor.close(); conn.close()
            return {'success': True, 'table_name': table_name, 'columns': columns, 'data': data,
                    'total_rows': total, 'preview_rows': len(data)}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _save_sync_task_sync(self, ds_id, source_table, target_table, schedule_type,
                             schedule_minute=0, schedule_hour=0, schedule_day_of_week=1,
                             schedule_day_of_month=1, enabled_for_ai=True):
        """保存同步任务 (同步)"""
        import uuid
        task_id = str(uuid.uuid4())[:8]
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = """INSERT INTO `_sys_sync_tasks` (`id`, `datasource_id`, `source_table`, `target_table`,
                 `schedule_type`, `schedule_minute`, `schedule_hour`, `schedule_day_of_week`,
                 `schedule_day_of_month`, `enabled_for_ai`, `status`, `created_at`)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'active', %s)"""
        self.db.execute_update(sql, (task_id, ds_id, source_table, target_table, schedule_type,
                                      schedule_minute, schedule_hour, schedule_day_of_week,
                                      schedule_day_of_month, 1 if enabled_for_ai else 0, now))
        return {'success': True, 'id': task_id, 'message': '同步任务已保存'}

    def _update_sync_task_sync(self, task_id, schedule_type, schedule_minute=0,
                               schedule_hour=0, schedule_day_of_week=1,
                               schedule_day_of_month=1, enabled_for_ai=True):
        """更新同步任务 (同步)"""
        sql = """UPDATE `_sys_sync_tasks` SET schedule_type = %s, schedule_minute = %s,
                 schedule_hour = %s, schedule_day_of_week = %s, schedule_day_of_month = %s,
                 enabled_for_ai = %s WHERE id = %s"""
        self.db.execute_update(sql, (schedule_type, schedule_minute, schedule_hour, schedule_day_of_week,
                                      schedule_day_of_month, 1 if enabled_for_ai else 0, task_id))
        return {'success': True, 'message': '任务已更新'}

    def _toggle_ai_enabled_sync(self, task_id, enabled):
        """切换AI启用状态 (同步)"""
        sql = "UPDATE `_sys_sync_tasks` SET enabled_for_ai = %s WHERE id = %s"
        self.db.execute_update(sql, (1 if enabled else 0, task_id))
        return {'success': True, 'enabled_for_ai': enabled, 'message': f'AI分析已{"启用" if enabled else "禁用"}'}


    def _list_sync_tasks_sync(self):
        """获取所有同步任务 (同步)"""
        sql = """
        SELECT t.*, d.name as datasource_name
        FROM `_sys_sync_tasks` t
        LEFT JOIN `_sys_datasources` d ON t.datasource_id = d.id
        ORDER BY t.created_at DESC
        """
        return self.db.execute_query(sql)

    def _get_ai_enabled_tables_sync(self):
        """获取启用AI的表 (同步)"""
        sql = """
        SELECT DISTINCT target_table
        FROM `_sys_sync_tasks`
        WHERE enabled_for_ai = 1
        """
        results = self.db.execute_query(sql)
        return [r['target_table'] for r in results]

    def _delete_sync_task_sync(self, task_id):
        """删除同步任务 (同步)"""
        sql = "DELETE FROM `_sys_sync_tasks` WHERE id = %s"
        self.db.execute_update(sql, (task_id,))
        return {'success': True, 'message': '同步任务已删除'}

    def _list_table_registry_sync(self):
        """获取表注册列表 (同步)"""
        sql = """
        SELECT
            r.table_name,
            r.display_name,
            r.description,
            r.source_type,
            r.created_at,
            r.updated_at,
            m.description AS auto_description,
            m.analyzed_at
        FROM `_sys_table_registry` r
        LEFT JOIN `_sys_table_metadata` m ON r.table_name = m.table_name
        ORDER BY COALESCE(m.analyzed_at, r.updated_at) DESC
        """
        return self.db.execute_query(sql)

    def _update_table_registry_sync(self, table_name, display_name=None, description=None):
        """更新表注册信息 (同步)"""
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = """
        UPDATE `_sys_table_registry`
        SET display_name = COALESCE(%s, display_name),
            description = COALESCE(%s, description),
            updated_at = %s
        WHERE table_name = %s
        """
        self.db.execute_update(sql, (display_name, description, now, table_name))
        return {'success': True, 'message': '表信息已更新'}



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

