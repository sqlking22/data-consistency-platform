#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/2/27
# @Author  : hejun
"""
简化的数据库连接池（基于DBUtils）

直接使用DBUtils管理底层连接，通过适配器包装
"""
import logging
import threading
from typing import Dict, Any, Optional, List, Tuple
from dbutils.pooled_db import PooledDB

logger = logging.getLogger(__name__)


class SimpleConnectionPool:
    """简化的数据库连接池"""

    def __init__(self, db_type: str, config: Dict[str, Any], max_connections: int = 5):
        """
        初始化连接池

        Args:
            db_type: 数据库类型
            config: 数据库连接配置
            max_connections: 最大连接数
        """
        self.db_type = db_type.lower()
        self.config = config
        self.max_connections = max_connections
        self._pool: Optional[PooledDB] = None
        self._lock = threading.Lock()

        # 生成连接池标识
        self.pool_id = f"{db_type}://{config.get('host')}:{config.get('port')}/{config.get('database')}"

        # 初始化连接池
        self._init_pool()

    def _init_pool(self):
        """初始化DBUtils连接池"""
        try:
            if self.db_type == 'mysql':
                import pymysql

                self._pool = PooledDB(
                    creator=pymysql,
                    maxconnections=self.max_connections,
                    mincached=1,
                    maxcached=3,
                    maxshared=2,
                    blocking=True,
                    maxusage=None,
                    setsession=[],
                    reset=True,
                    # 连接参数
                    host=self.config.get('host'),
                    port=self.config.get('port', 3306),
                    user=self.config.get('user'),
                    password=self.config.get('password'),
                    database=self.config.get('database'),
                    charset='utf8mb4',
                    connect_timeout=10,
                    read_timeout=30,
                    write_timeout=30,
                    cursorclass=pymysql.cursors.DictCursor
                )

                logger.info(f"[{self.pool_id}] MySQL连接池创建成功（最大{self.max_connections}个连接）")

            elif self.db_type == 'oracle':
                import cx_Oracle

                dsn = cx_Oracle.makedsn(
                    self.config.get('host'),
                    self.config.get('port'),
                    service_name=self.config.get('database')
                )

                self._pool = PooledDB(
                    creator=cx_Oracle,
                    maxconnections=self.max_connections,
                    mincached=1,
                    maxcached=3,
                    blocking=True,
                    user=self.config.get('user'),
                    password=self.config.get('password'),
                    dsn=dsn
                )

                logger.info(f"[{self.pool_id}] Oracle连接池创建成功")

            elif self.db_type == 'postgresql':
                import psycopg2
                import psycopg2.extras

                self._pool = PooledDB(
                    creator=psycopg2,
                    maxconnections=self.max_connections,
                    mincached=1,
                    maxcached=3,
                    blocking=True,
                    host=self.config.get('host'),
                    port=self.config.get('port', 5432),
                    user=self.config.get('user'),
                    password=self.config.get('password'),
                    database=self.config.get('database'),
                    connect_timeout=10,
                    cursor_factory=psycopg2.extras.DictCursor
                )

                logger.info(f"[{self.pool_id}] PostgreSQL连接池创建成功")

            elif self.db_type == 'sqlserver':
                import pymssql

                self._pool = PooledDB(
                    creator=pymssql,
                    maxconnections=self.max_connections,
                    mincached=1,
                    maxcached=3,
                    blocking=True,
                    host=self.config.get('host'),
                    port=self.config.get('port', 1433),
                    user=self.config.get('user'),
                    password=self.config.get('password'),
                    database=self.config.get('database'),
                    charset='utf8',
                    timeout=10
                )

                logger.info(f"[{self.pool_id}] SQLServer连接池创建成功")

            else:
                raise ValueError(f"不支持的数据库类型: {self.db_type}")

        except Exception as e:
            logger.error(f"[{self.pool_id}] 创建连接池失败: {str(e)}")
            raise

    def get_raw_connection(self):
        """获取底层连接"""
        if self._pool is None:
            raise RuntimeError(f"[{self.pool_id}] 连接池未初始化")

        try:
            conn = self._pool.connection()
            logger.debug(f"[{self.pool_id}] 获取连接成功")
            return conn
        except Exception as e:
            logger.error(f"[{self.pool_id}] 获取连接失败: {str(e)}")
            raise

    def close_all(self):
        """关闭连接池"""
        if self._pool:
            logger.info(f"[{self.pool_id}] 连接池已关闭")
            self._pool = None


class PooledAdapter:
    """连接池适配器包装器"""

    def __init__(self, pool: SimpleConnectionPool, config: Dict[str, Any]):
        """
        初始化包装器

        Args:
            pool: 连接池实例
            config: 数据库配置
        """
        self.pool = pool
        self.config = config
        self.connection = None
        self.cursor = None

        # 从连接池获取连接
        self._acquire_connection()

    def _acquire_connection(self):
        """从连接池获取连接"""
        self.connection = self.pool.get_raw_connection()
        if self.connection:
            self.cursor = self.connection.cursor()

    def query(self, sql: str, params: Tuple = None) -> List[Dict]:
        """执行查询"""
        try:
            self.cursor.execute(sql, params or ())
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"查询失败: SQL={sql}, 错误={str(e)}")
            raise

    def execute(self, sql: str, params: Tuple = None) -> int:
        """执行增删改"""
        try:
            affected_rows = self.cursor.execute(sql, params or ())
            self.connection.commit()
            return affected_rows
        except Exception as e:
            self.connection.rollback()
            logger.error(f"执行失败: SQL={sql}, 错误={str(e)}")
            raise

    def get_table_metadata(self, db_name: str, table_name: str) -> List[Dict]:
        """获取表元数据"""
        if self.pool.db_type == 'mysql':
            sql = """
                SELECT COLUMN_NAME as name, DATA_TYPE as type
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """
            return self.query(sql, (db_name, table_name))
        elif self.pool.db_type == 'oracle':
            sql = """
                SELECT COLUMN_NAME as name, DATA_TYPE as type
                FROM ALL_TAB_COLUMNS
                WHERE OWNER = UPPER(:1) AND TABLE_NAME = UPPER(:2)
            """
            return self.query(sql, (db_name.upper(), table_name.upper()))
        elif self.pool.db_type == 'postgresql':
            sql = """
                SELECT column_name as name, data_type as type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """
            return self.query(sql, (db_name, table_name))
        elif self.pool.db_type == 'sqlserver':
            sql = """
                SELECT COLUMN_NAME as name, DATA_TYPE as type
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_CATALOG = %s AND TABLE_NAME = %s
            """
            return self.query(sql, (db_name, table_name))

        raise ValueError(f"不支持的数据库类型: {self.pool.db_type}")

    def get_primary_keys(self, db_name: str, table_name: str) -> List[str]:
        """获取主键字段"""
        if self.pool.db_type == 'mysql':
            sql = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION
            """
            result = self.query(sql, (db_name, table_name))
            return [row['COLUMN_NAME'] for row in result]
        elif self.pool.db_type == 'oracle':
            sql = """
                SELECT cols.column_name
                FROM all_constraints cons
                JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name
                WHERE cons.owner = UPPER(:1) AND cons.table_name = UPPER(:2) AND cons.constraint_type = 'P'
                ORDER BY cols.position
            """
            result = self.query(sql, (db_name.upper(), table_name.upper()))
            return [row['COLUMN_NAME'] for row in result]
        elif self.pool.db_type == 'postgresql':
            sql = """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
            """
            result = self.query(sql, (f"{db_name}.{table_name}",))
            return [row['attname'] for row in result]
        elif self.pool.db_type == 'sqlserver':
            sql = """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_CATALOG = %s AND TABLE_NAME = %s AND CONSTRAINT_NAME LIKE 'PK%%'
                ORDER BY ORDINAL_POSITION
            """
            result = self.query(sql, (db_name, table_name))
            return [row['COLUMN_NAME'] for row in result]

        raise ValueError(f"不支持的数据库类型: {self.pool.db_type}")

    def get_table_count(self, db_name: str, table_name: str, where_clause: str = "") -> int:
        """获取表记录数"""
        sql = f"SELECT COUNT(*) as count FROM {db_name}.{table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        result = self.query(sql)
        return result[0]['count'] if result else 0

    def query_data(self, db_name: str, table_name: str, columns: List[str],
                   where_clause: str = "", limit: int = None) -> List[Dict]:
        """查询数据"""
        columns_str = ', '.join(columns)
        sql = f"SELECT {columns_str} FROM {db_name}.{table_name}"

        if where_clause:
            sql += f" WHERE {where_clause}"

        if limit:
            if self.pool.db_type == 'mysql':
                sql += f" LIMIT {limit}"
            elif self.pool.db_type == 'oracle':
                sql = f"SELECT * FROM ({sql}) WHERE ROWNUM <= {limit}"
            elif self.pool.db_type == 'postgresql':
                sql += f" LIMIT {limit}"
            elif self.pool.db_type == 'sqlserver':
                sql = f"SELECT TOP {limit} * FROM ({sql}) t"

        return self.query(sql)

    def close(self):
        """关闭连接（归还到连接池）"""
        try:
            if self.cursor:
                self.cursor.close()
                self.cursor = None
            # DBUtils会自动管理连接，不需要手动关闭
            self.connection = None
            logger.debug(f"[{self.pool.pool_id}] 连接已归还到连接池")
        except Exception as e:
            logger.warning(f"关闭连接时出错: {str(e)}")


class ConnectionPoolManager:
    """连接池管理器（单例模式）"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._pools: Dict[str, SimpleConnectionPool] = {}
        return cls._instance

    def get_pool(self, config: Dict[str, Any], max_connections: int = 5) -> SimpleConnectionPool:
        """获取或创建连接池"""
        db_type = config.get('db_type', 'mysql')

        # 生成连接池唯一键
        pool_key = f"{db_type}:{config.get('host')}:{config.get('port')}:{config.get('database')}"

        if pool_key not in self._pools:
            with threading.Lock():  # 使用临时锁确保线程安全
                if pool_key not in self._pools:
                    logger.info(f"创建新的连接池：{pool_key}")
                    self._pools[pool_key] = SimpleConnectionPool(db_type, config, max_connections)

        return self._pools[pool_key]

    def close_all(self):
        """关闭所有连接池"""
        logger.info("关闭所有连接池")
        for pool in self._pools.values():
            pool.close_all()
        self._pools.clear()


# 全局连接池管理器
_pool_manager = ConnectionPoolManager()


def get_pooled_connection(config: Dict[str, Any], max_connections: int = 5) -> PooledAdapter:
    """获取连接池中的连接"""
    pool = _pool_manager.get_pool(config, max_connections)
    return PooledAdapter(pool, config)


def return_pooled_connection(config: Dict[str, Any], connection: PooledAdapter):
    """归还连接到连接池"""
    if connection:
        connection.close()


def get_pool_manager() -> ConnectionPoolManager:
    """获取全局连接池管理器"""
    return _pool_manager
