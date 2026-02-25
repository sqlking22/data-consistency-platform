#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 16:22
# @Author  : hejun
import cx_Oracle
import logging
from typing import List, Dict, Tuple, Any
from core.db_adapter.base_adapter import BaseDBAdapter
from utils.retry_utils import retry_decorator

logger = logging.getLogger(__name__)


class OracleAdapter(BaseDBAdapter):
    def connect(self):
        """建立Oracle连接"""
        try:
            dsn = cx_Oracle.makedsn(
                self.config.get('host'),
                self.config.get('port', 1521),
                service_name=self.config.get('database')
            )
            self.connection = cx_Oracle.connect(
                user=self.config.get('user'),
                password=self.config.get('password'),
                dsn=dsn,
                encoding='UTF-8'
            )
            self.cursor = self.connection.cursor()
            logger.info(
                f"成功连接Oracle数据库：{self.config.get('host')}:{self.config.get('port')}/{self.config.get('database')}"
            )
        except Exception as e:
            logger.error(f"Oracle连接失败：{str(e)}")
            raise

    def close(self):
        """关闭连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Oracle连接已关闭")

    @retry_decorator(max_retries=3, delay=5)
    def query(self, sql: str, params: Tuple = None) -> List[Dict]:
        """执行查询"""
        try:
            # Oracle使用命名参数或位置参数
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
            
            # 获取列名
            columns = [desc[0] for desc in self.cursor.description]
            rows = self.cursor.fetchall()
            
            # 转换为字典列表
            result = []
            for row in rows:
                result.append(dict(zip(columns, row)))
            return result
        except Exception as e:
            logger.error(f"Oracle查询失败：SQL={sql}, 参数={params}, 错误={str(e)}")
            raise

    @retry_decorator(max_retries=3, delay=5)
    def execute(self, sql: str, params: Tuple = None) -> int:
        """执行增删改"""
        try:
            if params:
                affected_rows = self.cursor.execute(sql, params)
            else:
                affected_rows = self.cursor.execute(sql)
            self.connection.commit()
            return self.cursor.rowcount
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Oracle执行失败：SQL={sql}, 参数={params}, 错误={str(e)}")
            raise

    def get_table_metadata(self, db_name: str, table_name: str) -> List[Dict]:
        """获取表元数据"""
        sql = """
            SELECT COLUMN_NAME as name, DATA_TYPE as type 
            FROM ALL_TAB_COLUMNS 
            WHERE OWNER = UPPER(%s) AND TABLE_NAME = UPPER(%s)
        """
        return self.query(sql, (db_name, table_name))

    def get_primary_keys(self, db_name: str, table_name: str) -> List[str]:
        """获取主键字段"""
        sql = """
            SELECT ac.COLUMN_NAME 
            FROM ALL_CONSTRAINTS ac
            JOIN ALL_CONS_COLUMNS acc ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
            WHERE ac.TABLE_NAME = UPPER(:1) 
            AND ac.CONSTRAINT_TYPE = 'P' 
            AND ac.OWNER = UPPER(:2)
            ORDER BY acc.POSITION
        """
        result = self.query(sql, (table_name, db_name))
        return [row['COLUMN_NAME'] for row in result]

    def get_table_count(self, db_name: str, table_name: str, where_clause: str = "") -> int:
        """获取表记录数"""
        sql = f"SELECT COUNT(*) as count FROM {db_name}.{table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        result = self.query(sql)
        return result[0]['COUNT'] if result else 0

    def query_data(self, db_name: str, table_name: str, columns: List[str], where_clause: str = "",
                   limit: int = None) -> List[Dict]:
        """查询数据"""
        columns_str = ', '.join(columns)
        sql = f"SELECT {columns_str} FROM {db_name}.{table_name}"
        if where_clause and limit:
            sql += f" WHERE {where_clause} AND ROWNUM <= {limit}"
        elif where_clause:
            sql += f" WHERE {where_clause}"
        elif limit:
            sql += f" WHERE ROWNUM <= {limit}"
        return self.query(sql)