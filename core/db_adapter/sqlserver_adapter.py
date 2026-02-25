#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 16:24
# @Author  : hejun
import pyodbc
import logging
from typing import List, Dict, Tuple, Any
from core.db_adapter.base_adapter import BaseDBAdapter
from utils.retry_utils import retry_decorator

logger = logging.getLogger(__name__)


class SQLServerAdapter(BaseDBAdapter):
    def connect(self):
        """建立SQL Server连接"""
        try:
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.config.get('host')},{self.config.get('port', 1433)};"
                f"DATABASE={self.config.get('database')};"
                f"UID={self.config.get('user')};"
                f"PWD={self.config.get('password')}"
            )
            self.connection = pyodbc.connect(connection_string)
            self.cursor = self.connection.cursor()
            logger.info(
                f"成功连接SQL Server数据库：{self.config.get('host')}:{self.config.get('port')}/{self.config.get('database')}"
            )
        except Exception as e:
            logger.error(f"SQL Server连接失败：{str(e)}")
            raise

    def close(self):
        """关闭连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("SQL Server连接已关闭")

    @retry_decorator(max_retries=3, delay=5)
    def query(self, sql: str, params: Tuple = None) -> List[Dict]:
        """执行查询"""
        try:
            self.cursor.execute(sql, params or ())
            columns = [column[0] for column in self.cursor.description]
            rows = self.cursor.fetchall()
            
            # 转换为字典列表
            result = []
            for row in rows:
                result.append(dict(zip(columns, row)))
            return result
        except Exception as e:
            logger.error(f"SQL Server查询失败：SQL={sql}, 参数={params}, 错误={str(e)}")
            raise

    @retry_decorator(max_retries=3, delay=5)
    def execute(self, sql: str, params: Tuple = None) -> int:
        """执行增删改"""
        try:
            affected_rows = self.cursor.execute(sql, params or ())
            self.connection.commit()
            return self.cursor.rowcount
        except Exception as e:
            self.connection.rollback()
            logger.error(f"SQL Server执行失败：SQL={sql}, 参数={params}, 错误={str(e)}")
            raise

    def get_table_metadata(self, db_name: str, table_name: str) -> List[Dict]:
        """获取表元数据"""
        sql = """
            SELECT COLUMN_NAME as name, DATA_TYPE as type 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """
        return self.query(sql, (db_name, table_name))

    def get_primary_keys(self, db_name: str, table_name: str) -> List[str]:
        """获取主键字段"""
        sql = """
            SELECT ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                AND tc.TABLE_NAME = ku.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.TABLE_NAME = ?
                AND ku.TABLE_SCHEMA = ?
            ORDER BY ku.ORDINAL_POSITION
        """
        result = self.query(sql, (table_name, db_name))
        return [row['COLUMN_NAME'] for row in result]

    def get_table_count(self, db_name: str, table_name: str, where_clause: str = "") -> int:
        """获取表记录数"""
        sql = f"SELECT COUNT(*) as count FROM [{db_name}].[{table_name}]"
        if where_clause:
            sql += f" WHERE {where_clause}"
        result = self.query(sql)
        return result[0]['count'] if result else 0

    def query_data(self, db_name: str, table_name: str, columns: List[str], where_clause: str = "",
                   limit: int = None) -> List[Dict]:
        """查询数据"""
        columns_str = ', '.join([f'[{col}]' for col in columns])
        top_clause = f" TOP {limit}" if limit else ""
        sql = f"SELECT{top_clause} {columns_str} FROM [{db_name}].[{table_name}]"
        if where_clause:
            sql += f" WHERE {where_clause}"
        return self.query(sql)