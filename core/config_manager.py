#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 12:42
# @Author  : hejun
import json
import os
import argparse
from typing import Dict, Any
from core.db_adapter.base_adapter import get_db_adapter
from config.settings import TASK_DB_CONFIG, TASK_CONFIG_TABLE


class ConfigManager:
    """配置管理中心，实现配置加载优先级：命令行 > settings.py > JSON/数据库"""

    def __init__(self):
        self.global_config = {}  # 存储全局参数（命令行+settings.py）
        self.task_config = {}  # 存储任务参数（JSON/数据库）
        self.args = None

    def load_global_config(self):
        """加载全局配置（命令行 > settings.py）"""
        # 加载命令行参数
        self.load_cli_args()

        # 从 settings.py 加载全局配置
        from config.settings import (
            MAX_THREAD_COUNT, WX_ALERT_THRESHOLD,
            ENABLE_REPAIR, IS_INCREMENTAL, INCREMENTAL_DAYS,
            TIME_TOLERANCE, RETRY_TIMES, RETRY_DELAY,
            BATCH_SIZE, MAX_RECORDS_THRESHOLD, MAX_DIFF_RECORDS_THRESHOLD,
            MAX_REPAIR_RECORDS_THRESHOLD, CHUNK_SIZE_FOR_DATA_SYNC,
            RECORDS_PER_THREAD, ENGINE_STRATEGY, DECODE_PASSWORD_FLAG,
            EXTRA_COLUMN_FLAG
        )

        # 命令行参数优先级高于 settings.py
        self.global_config = {
            'concurrency': getattr(self.args, 'concurrency', MAX_THREAD_COUNT) if hasattr(self.args,
                                                                                          'concurrency') and self.args.concurrency is not None else MAX_THREAD_COUNT,
            'incremental': getattr(self.args, 'incremental', IS_INCREMENTAL) if hasattr(self.args,
                                                                                        'incremental') and self.args.incremental is not None else IS_INCREMENTAL,
            'incremental_days': getattr(self.args, 'incremental_days', INCREMENTAL_DAYS) if hasattr(self.args,
                                                                                                    'incremental_days') and self.args.incremental_days is not None else INCREMENTAL_DAYS,
            'enable_repair': getattr(self.args, 'enable_repair', ENABLE_REPAIR) if hasattr(self.args,
                                                                                           'enable_repair') and self.args.enable_repair is not None else ENABLE_REPAIR,
            'time_tolerance': TIME_TOLERANCE,
            'retry_times': RETRY_TIMES,
            'retry_delay': RETRY_DELAY,
            'batch_size': BATCH_SIZE,
            'max_records_threshold': MAX_RECORDS_THRESHOLD,
            'max_diff_records_threshold': MAX_DIFF_RECORDS_THRESHOLD,
            'max_repair_records_threshold': MAX_REPAIR_RECORDS_THRESHOLD,
            'chunk_size_for_data_sync': CHUNK_SIZE_FOR_DATA_SYNC,
            'records_per_thread': RECORDS_PER_THREAD,
            'engine_strategy': ENGINE_STRATEGY,
            'decode_password_flag': DECODE_PASSWORD_FLAG,
            'extra_column_flag': EXTRA_COLUMN_FLAG,
            'alert_threshold': WX_ALERT_THRESHOLD,
            'config_file': getattr(self.args, 'config_file', 'config/config.json') if hasattr(self.args,
                                                                                              'config_file') and self.args.config_file is not None else 'config/config.json'
        }

    def load_task_config(self):
        """加载任务配置（JSON > 数据库）"""
        config_file = self.global_config.get('config_file', 'config/config.json')

        # 尝试加载JSON配置
        if self.load_json_config_if_exists(config_file):
            # JSON配置存在且有效，使用JSON配置
            pass
        else:
            # JSON配置不存在或无效，从数据库加载
            self.load_db_config()

    def load_json_config_if_exists(self, file_path: str) -> bool:
        """尝试加载JSON配置文件，返回是否成功"""
        import os
        import json
        if not os.path.exists(file_path):
            return False

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                json_config = json.load(f)

            # 验证必要参数是否存在
            required_fields = [
                'src_db_type', 'src_host', 'src_port', 'src_username',
                'src_password', 'src_db_name', 'src_table_name',
                'tgt_db_type', 'tgt_host', 'tgt_port', 'tgt_username',
                'tgt_password', 'tgt_db_name', 'tgt_table_name'
            ]

            missing_fields = [field for field in required_fields if field not in json_config]
            if missing_fields:
                print(f"JSON配置文件缺少必要字段: {missing_fields}")
                return False

            self.task_config = json_config
            return True
        except Exception as e:
            print(f"JSON配置文件加载失败: {e}")
            return False

    def load_db_config(self, table_id: int = None):
        """从数据库加载配置"""
        table_id = table_id or self.global_config.get('table_id')

        adapter = get_db_adapter(TASK_DB_CONFIG)
        try:
            if table_id:
                sql = f"""
                    SELECT * FROM {TASK_CONFIG_TABLE} 
                    WHERE id = %s AND is_delete = 0
                """
                result = adapter.query(sql, (table_id,))
                if result:
                    self.task_config = dict(result[0])
            else:
                sql = f"""
                    SELECT * FROM {TASK_CONFIG_TABLE} 
                    WHERE is_delete = 0
                """
                result = adapter.query(sql)
                # 如果没有指定table_id，存储所有配置
                self.task_config = [dict(row) for row in result] if result else []
        finally:
            adapter.close()

    def load_cli_args(self):
        """加载命令行参数"""
        parser = argparse.ArgumentParser(description='跨库数据一致性校验工具')
        parser.add_argument('--table_id', type=int, help='表ID')
        parser.add_argument('--config_file', type=str, help='JSON配置文件路径')
        parser.add_argument('--incremental', default=False, help='是否增量比对')
        parser.add_argument('--incremental_days', type=int,default=1, help='增量比对天数')
        parser.add_argument('--concurrency', type=int, default=5, help='批量执行并发数')
        parser.add_argument('--enable_repair', default=True, help='是否启用修复')

        self.args = parser.parse_args()

    def decrypt_password(self):
        """解密数据库密码（如需）"""
        from config.settings import DECODE_PASSWORD_FLAG
        if not DECODE_PASSWORD_FLAG:
            return

        if 'src_password' in self.task_config and self.task_config['src_password']:
            from utils.crypto_utils import decrypt
            self.task_config['src_password'] = decrypt(self.task_config['src_password'])

        if 'tgt_password' in self.task_config and self.task_config['tgt_password']:
            from utils.crypto_utils import decrypt
            self.task_config['tgt_password'] = decrypt(self.task_config['tgt_password'])

    def load_all_configs(self):
        """加载所有配置"""
        # 1. 加载全局配置（命令行 > settings.py）
        self.load_global_config()

        # 2. 加载任务配置（JSON > 数据库）
        self.load_task_config()

        # 3. 解密密码
        self.decrypt_password()

        # 4. 合并配置
        final_config = self.global_config.copy()
        # 如果task_config是列表（多任务），则返回全局配置和任务列表
        if isinstance(self.task_config, list):
            # 多任务模式，返回全局配置和任务列表
            return {
                'global_config': final_config,
                'task_configs': self.task_config
            }
        else:
            # 单任务模式，合并全局配置和任务配置
            final_config.update(self.task_config)
            return final_config
