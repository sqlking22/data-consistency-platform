#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 16:15
# @Author  : hejun
import pymysql
import logging
from typing import List, Dict, Tuple, Any
from core.db_adapter.base_adapter import BaseDBAdapter

logger = logging.getLogger(__name__)


class MySQLAdapter(BaseDBAdapter):
    def connect(self):
        """建立MySQL连接"""
        try:
            self.connection = pymysql.connect(
                host=self.config.get('host'),
                port=self.config.get('port', 3306),
                user=self.config.get('user'),
                password=self.config.get('password'),
                database=self.config.get('database'),
                charset='utf8mb4',
                connect_timeout=30
            )
            self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)
            logger.info(
                f"成功连接MySQL数据库：{self.config.get('host')}:{self.config.get('port')}/{self.config.get('database')}")
        except Exception as e:
            logger.error(f"MySQL连接失败：{str(e)}")
            raise

    def close(self):
        """关闭连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("MySQL连接已关闭")

    def query(self, sql: str, params: Tuple = None) -> List[Dict]:
        """执行查询"""
        try:
            self.cursor.execute(sql, params or ())
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"MySQL查询失败：SQL={sql}, 参数={params}, 错误={str(e)}")
            raise

    def execute(self, sql: str, params: Tuple = None) -> int:
        """执行增删改"""
        try:
            affected_rows = self.cursor.execute(sql, params or ())
            self.connection.commit()
            return affected_rows
        except Exception as e:
            self.connection.rollback()
            logger.error(f"MySQL执行失败：SQL={sql}, 参数={params}, 错误={str(e)}")
            raise

    def get_table_metadata(self, db_name: str, table_name: str) -> List[Dict]:
        """获取表元数据"""
        sql = f"""
            SELECT COLUMN_NAME as name, DATA_TYPE as type 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """
        return self.query(sql, (db_name, table_name))

    def get_primary_keys(self, db_name: str, table_name: str) -> List[str]:
        """获取主键字段"""
        sql = f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """
        result = self.query(sql, (db_name, table_name))
        return [row['COLUMN_NAME'] for row in result]

    def get_table_count(self, db_name: str, table_name: str, where_clause: str = "") -> int:
        """获取表记录数"""
        sql = f"SELECT COUNT(*) as count FROM {db_name}.{table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        result = self.query(sql)
        return result[0]['count'] if result else 0

    def query_data(self, db_name: str, table_name: str, columns: List[str], where_clause: str = "",
                   limit: int = None) -> List[Dict]:
        """查询数据"""
        columns_str = ', '.join(columns)
        sql = f"SELECT {columns_str} FROM {db_name}.{table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        if limit:
            sql += f" LIMIT {limit}"

        return self.query(sql)