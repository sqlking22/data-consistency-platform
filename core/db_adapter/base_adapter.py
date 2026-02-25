#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 12:43
# @Author  : hejun
from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Any
import time
from utils.retry_utils import retry_decorator


class BaseDBAdapter(ABC):
    """数据库适配器基类"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.cursor = None
        self.connect()

    @abstractmethod
    def connect(self):
        """建立数据库连接"""
        pass

    @abstractmethod
    def close(self):
        """关闭数据库连接"""
        pass

    @abstractmethod
    @retry_decorator(max_retries=3, delay=5)
    def query(self, sql: str, params: Tuple = None) -> List[Dict]:
        """执行查询"""
        pass

    @abstractmethod
    @retry_decorator(max_retries=3, delay=5)
    def execute(self, sql: str, params: Tuple = None) -> int:
        """执行增删改"""
        pass

    @abstractmethod
    def get_table_metadata(self, db_name: str, table_name: str) -> List[Dict]:
        """获取表元数据"""
        pass

    @abstractmethod
    def get_primary_keys(self, db_name: str, table_name: str) -> List[str]:
        """获取主键字段"""
        pass

    @abstractmethod
    def get_table_count(self, db_name: str, table_name: str, where_clause: str = "") -> int:
        """获取表记录数"""
        pass

    @abstractmethod
    def query_data(self, db_name: str, table_name: str, columns: List[str],
                   where_clause: str = "", limit: int = None) -> List[Dict]:
        """查询数据"""
        pass

    def get_extra_columns(self, db_name: str, table_name: str) -> List[str]:
        """获取额外比对字段（根据字段类型筛选）"""
        from config.settings import SUPPORT_COLUMN_TYPE, EXTRA_COLUMN_FLAG
        if not EXTRA_COLUMN_FLAG:
            return []

        metadata = self.get_table_metadata(db_name, table_name)
        db_type = self.config.get('db_type', '').lower()
        supported_types = SUPPORT_COLUMN_TYPE.get(db_type, set())

        extra_columns = []
        for col_info in metadata:
            col_type = col_info['type'].lower()
            # 匹配字段类型
            if any(st in col_type for st in supported_types):
                extra_columns.append(col_info['name'])

        return extra_columns


def get_db_adapter(config: Dict[str, Any]) -> BaseDBAdapter:
    """获取数据库适配器实例"""
    db_type = config.get('db_type', '').lower()

    if db_type == 'mysql':
        from core.db_adapter.mysql_adapter import MySQLAdapter
        return MySQLAdapter(config)
    elif db_type == 'oracle':
        from core.db_adapter.oracle_adapter import OracleAdapter
        return OracleAdapter(config)
    elif db_type == 'postgresql':
        from core.db_adapter.postgres_adapter import PostgresAdapter
        return PostgresAdapter(config)
    elif db_type == 'sqlserver':
        from core.db_adapter.sqlserver_adapter import SQLServerAdapter
        return SQLServerAdapter(config)
    else:
        raise ValueError(f"不支持的数据库类型: {db_type}")