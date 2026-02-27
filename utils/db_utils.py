#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 16:13
# @Author  : hejun
import logging
from typing import Dict, Any
from core.db_adapter.base_adapter import get_db_adapter

logger = logging.getLogger(__name__)


def write_task_log(db_config: Dict[str, Any], table_name: str, log_data: Dict[str, Any]):
    """写入任务日志到数据库"""
    adapter = None
    try:
        adapter = get_db_adapter(db_config)
        # 构建插入SQL
        columns = [k for k, v in log_data.items() if v is not None]
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({placeholders})
        """
        values = [log_data[k] for k in columns]

        # 执行插入
        adapter.execute(sql, tuple(values))
        logger.debug(f"日志写入成功：配置ID={log_data.get('config_id')}")
    except Exception as e:
        logger.error(f"写入任务日志失败：{str(e)}")
        raise
    finally:
        if 'adapter' in locals():
            adapter.close()


def get_table_exists(adapter, db_name: str, table_name: str) -> bool:
    """检查表是否存在

    @deprecated 此函数目前未被使用，仅保留用于测试
    """
    import warnings
    warnings.warn("get_table_exists is deprecated and will be removed in future versions",
                  DeprecationWarning, stacklevel=2)
    try:
        adapter.get_table_metadata(db_name, table_name)
        return True
    except Exception:
        return False


def get_table_writable(adapter, db_name: str, table_name: str) -> bool:
    """检查表是否可写

    @deprecated 此函数目前未被使用，仅保留用于测试
    """
    import warnings
    warnings.warn("get_table_writable is deprecated and will be removed in future versions",
                  DeprecationWarning, stacklevel=2)
    try:
        # 执行测试更新（事务回滚）
        test_sql = f"UPDATE {db_name}.{table_name} SET 1=1 WHERE 1=0"
        adapter.execute(test_sql)
        return True
    except Exception as e:
        logger.warning(f"表{table_name}不可写：{str(e)}")
        return False