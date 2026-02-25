#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 16:23
# @Author  : hejun
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from typing import List, Dict, Tuple, Any
from core.db_adapter.base_adapter import BaseDBAdapter
from utils.retry_utils import retry_decorator

logger = logging.getLogger(__name__)


class PostgresAdapter(BaseDBAdapter):
    def connect(self):
        """建立PostgreSQL连接"""
        try:
            self.connection = psycopg2.connect(
                host=self.config.get('host'),
                port=self.config.get('port', 5432),
                user=self.config.get('user'),
                password=self.config.get('password'),
                database=self.config.get('database')
            )
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            logger.info(
                f"成功连接PostgreSQL数据库：{self.config.get('host')}:{self.config.get('port')}/{self.config.get('database')}"
            )
        except Exception as e:
            logger.error(f"PostgreSQL连接失败：{str(e)}")
            raise

    def close(self):
        """关闭连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("PostgreSQL连接已关闭")

    @retry_decorator(max_retries=3, delay=5)
    def query(self, sql: str, params: Tuple = None) -> List[Dict]:
        """执行查询"""
        try:
            self.cursor.execute(sql, params or ())
            return [dict(row) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"PostgreSQL查询失败：SQL={sql}, 参数={params}, 错误={str(e)}")
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
            logger.error(f"PostgreSQL执行失败：SQL={sql}, 参数={params}, 错误={str(e)}")
            raise

    def get_table_metadata(self, db_name: str, table_name: str) -> List[Dict]:
        """获取表元数据"""
        sql = """
            SELECT column_name as name, data_type as type 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
        """
        return self.query(sql, (db_name, table_name))

    def get_primary_keys(self, db_name: str, table_name: str) -> List[str]:
        """获取主键字段"""
        sql = """
            SELECT kcu.column_name 
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name 
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = %s 
            AND tc.table_name = %s 
            AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
        """
        result = self.query(sql, (db_name, table_name))
        return [row['column_name'] for row in result]

    def get_table_count(self, db_name: str, table_name: str, where_clause: str = "") -> int:
        """获取表记录数"""
        sql = f'SELECT COUNT(*) as count FROM "{db_name}"."{table_name}"'
        if where_clause:
            sql += f" WHERE {where_clause}"
        result = self.query(sql)
        return result[0]['count'] if result else 0

    def query_data(self, db_name: str, table_name: str, columns: List[str], where_clause: str = "",
                   limit: int = None) -> List[Dict]:
        """查询数据"""
        columns_str = ', '.join([f'"{col}"' for col in columns])
        sql = f'SELECT {columns_str} FROM "{db_name}"."{table_name}"'
        if where_clause:
            sql += f" WHERE {where_clause}"
        if limit:
            sql += f" LIMIT {limit}"

        return self.query(sql)