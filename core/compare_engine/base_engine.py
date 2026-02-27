#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 12:43
# @Author  : hejun
from abc import ABC, abstractmethod
from typing import Dict, List, Any
import pandas as pd
from datetime import datetime, timedelta
from core.db_adapter.base_adapter import BaseDBAdapter


class BaseCompareEngine(ABC):
    """比对引擎基类"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.src_adapter: BaseDBAdapter = None
        self.tgt_adapter: BaseDBAdapter = None
        self.src_df: pd.DataFrame = None
        self.tgt_df: pd.DataFrame = None
        self.compare_result: Dict[str, Any] = {
            'src_cnt': 0,
            'tgt_cnt': 0,
            'diff_cnt': 0,
            'compare_report': '',
            'html_report': '',
            'matching_rate': 0.0
        }
        self.start_time: datetime = None
        self.end_time: datetime = None

        # 元数据缓存（避免重复查询）
        self._src_metadata_cache: List[Dict] = None
        self._tgt_metadata_cache: List[Dict] = None
        self._src_pk_cache: List[str] = None
        self._tgt_pk_cache: List[str] = None
        self._compare_columns_cache: Dict[str, List[str]] = None

    def init_adapters(self):
        """初始化源端和目标端数据库适配器（使用连接池）"""
        # 使用新的简化连接池
        from utils.db_connection_pool import get_pooled_connection

        # 源端适配器
        src_config = {
            'db_type': self.config['src_db_type'],
            'host': self.config['src_host'],
            'port': self.config['src_port'],
            'user': self.config['src_username'],
            'password': self.config['src_password'],
            'database': self.config['src_db_name']
        }
        self.src_adapter = get_pooled_connection(src_config)

        # 目标端适配器
        tgt_config = {
            'db_type': self.config['tgt_db_type'],
            'host': self.config['tgt_host'],
            'port': self.config['tgt_port'],
            'user': self.config['tgt_username'],
            'password': self.config['tgt_password'],
            'database': self.config['tgt_db_name']
        }
        self.tgt_adapter = get_pooled_connection(tgt_config)

        # 缓存元数据和主键信息（避免重复查询）
        self._cache_metadata()

    def _cache_metadata(self):
        """缓存表元数据和主键信息（性能优化：避免重复查询数据库）"""
        import logging
        logger = logging.getLogger(__name__)

        logger.info("开始缓存表元数据...")

        try:
            # 缓存源端元数据和主键
            logger.debug("正在获取源端表元数据...")
            self._src_metadata_cache = self.src_adapter.get_table_metadata(
                self.config['src_db_name'],
                self.config['src_table_name']
            )
            logger.debug(f"源端元数据获取完成: {len(self._src_metadata_cache)}个字段")

            logger.debug("正在获取源端主键...")
            self._src_pk_cache = self.src_adapter.get_primary_keys(
                self.config['src_db_name'],
                self.config['src_table_name']
            )
            logger.debug(f"源端主键获取完成: {self._src_pk_cache}")

            # 缓存目标端元数据和主键
            logger.debug("正在获取目标端表元数据...")
            self._tgt_metadata_cache = self.tgt_adapter.get_table_metadata(
                self.config['tgt_db_name'],
                self.config['tgt_table_name']
            )
            logger.debug(f"目标端元数据获取完成: {len(self._tgt_metadata_cache)}个字段")

            logger.debug("正在获取目标端主键...")
            self._tgt_pk_cache = self.tgt_adapter.get_primary_keys(
                self.config['tgt_db_name'],
                self.config['tgt_table_name']
            )
            logger.debug(f"目标端主键获取完成: {self._tgt_pk_cache}")

            logger.info(f"元数据缓存完成：源端{len(self._src_metadata_cache)}个字段，目标端{len(self._tgt_metadata_cache)}个字段")

        except Exception as e:
            logger.error(f"缓存元数据失败: {str(e)}")
            raise

    def get_where_clause(self) -> str:
        """构建WHERE子句（增量/全量）"""
        is_incremental = self.config.get('incremental', False)
        if not is_incremental or not self.config.get('update_time_str'):
            return ""

        # 增量比对：根据更新时间字段
        incremental_days = self.config.get('incremental_days', 1)
        end_time = datetime.now()
        start_time = end_time - timedelta(days=incremental_days)

        # 记录检查范围
        self.compare_result[
            'check_range'] = f"[{start_time.strftime('%Y-%m-%d %H:%M:%S')},{end_time.strftime('%Y-%m-%d %H:%M:%S')})"

        update_col = self.config['update_time_str']
        db_type = self.config['src_db_type'].lower()

        # 不同数据库的时间格式处理
        if db_type == 'mysql':
            return f"{update_col} >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND {update_col} < '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif db_type == 'oracle':
            return f"{update_col} >= TO_DATE('{start_time.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS') AND {update_col} < TO_DATE('{end_time.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
        elif db_type == 'sqlserver':
            return f"{update_col} >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND {update_col} < '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'"
        elif db_type == 'postgresql':
            return f"{update_col} >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND {update_col} < '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'"

        return ""

    def get_compare_columns(self) -> Dict[str, List[str]]:
        """获取比对字段（使用缓存，避免重复查询）"""
        # 如果已有缓存，直接返回
        if self._compare_columns_cache is not None:
            return self._compare_columns_cache

        # 获取主键（从缓存）
        pk_columns = self._src_pk_cache
        if not pk_columns:
            raise ValueError("表无主键，无法进行比对")

        # 更新时间字段
        update_column = []
        if self.config.get('update_time_str'):
            update_column = [self.config['update_time_str']]

        # 额外字段（从缓存的元数据中筛选）
        from config.settings import SUPPORT_COLUMN_TYPE, EXTRA_COLUMN_FLAG
        extra_columns = []
        if EXTRA_COLUMN_FLAG and self._src_metadata_cache:
            db_type = self.config.get('src_db_type', '').lower()
            supported_types = SUPPORT_COLUMN_TYPE.get(db_type, set())

            for col_info in self._src_metadata_cache:
                col_type = col_info['type'].lower()
                # 匹配字段类型
                if any(st in col_type for st in supported_types):
                    extra_columns.append(col_info['name'])

        # 排除敏感字段
        sensitive_fields = self.config.get('sensitive_str', '').split(',') if self.config.get('sensitive_str') else []
        sensitive_fields = [f.strip() for f in sensitive_fields if f.strip()]

        # 过滤敏感字段
        pk_columns = [col for col in pk_columns if col not in sensitive_fields]
        update_column = [col for col in update_column if col not in sensitive_fields]
        extra_columns = [col for col in extra_columns if col not in sensitive_fields]

        columns = {
            'key_columns': pk_columns,
            'update_column': update_column,
            'extra_columns': extra_columns
        }

        # 缓存结果
        self._compare_columns_cache = columns

        # 记录检查字段
        self.compare_result[
            'check_column'] = f"key_columns：{pk_columns},update_column: {update_column},extra_columns: {extra_columns}"
        # 添加结构化字段信息供修复引擎使用
        self.compare_result['compare_columns'] = {
            'key_columns': pk_columns,
            'update_column': update_column,
            'extra_columns': extra_columns
        }

        return columns

    @abstractmethod
    def load_data(self):
        """加载源端和目标端数据"""
        pass

    @abstractmethod
    def compare(self) -> Dict[str, Any]:
        """执行比对"""
        pass

    def generate_report(self) -> str:
        """生成比对报告"""
        from utils.report_utils import generate_html_report
        self.compare_result['html_report'] = generate_html_report(self.compare_result)
        return self.compare_result['compare_report']

    def run(self) -> Dict[str, Any]:
        """执行完整比对流程"""
        self.start_time = datetime.now()

        try:
            # 初始化适配器
            self.init_adapters()

            # 加载数据
            self.load_data()

            # 执行比对
            self.compare()

        except Exception as e:
            self.compare_result['compare_status'] = 'fail'
            self.compare_result['compare_msg'] = str(e)
            raise
        finally:
            self.end_time = datetime.now()
            # 计算耗时
            cost_seconds = (self.end_time - self.start_time).total_seconds()
            self.compare_result['compare_start_time'] = self.start_time
            self.compare_result['compare_end_time'] = self.end_time
            self.compare_result['compare_cost_minute'] = round(cost_seconds / 60, 6)
            # 设置比对时间（用于HTML报告）
            self.compare_result['compare_time'] = self.start_time.strftime('%Y-%m-%d %H:%M:%S')

            # 如果比对成功，设置成功状态
            if 'compare_status' not in self.compare_result or not self.compare_result['compare_status']:
                self.compare_result['compare_status'] = 'success'
                self.compare_result['compare_msg'] = '比对成功'

            # 生成报告（在所有必要字段设置完成之后）
            self.generate_report()

            # 关闭连接（归还到连接池）
            if self.src_adapter:
                from utils.db_connection_pool import return_pooled_connection
                src_config = {
                    'db_type': self.config['src_db_type'],
                    'host': self.config['src_host'],
                    'port': self.config['src_port'],
                    'user': self.config['src_username'],
                    'password': self.config['src_password'],
                    'database': self.config['src_db_name']
                }
                return_pooled_connection(src_config, self.src_adapter)
            if self.tgt_adapter:
                from utils.db_connection_pool import return_pooled_connection
                tgt_config = {
                    'db_type': self.config['tgt_db_type'],
                    'host': self.config['tgt_host'],
                    'port': self.config['tgt_port'],
                    'user': self.config['tgt_username'],
                    'password': self.config['tgt_password'],
                    'database': self.config['tgt_db_name']
                }
                return_pooled_connection(tgt_config, self.tgt_adapter)
        return self.compare_result


def get_compare_engine(config: Dict[str, Any]) -> BaseCompareEngine:
    """根据数据量选择合适的比对引擎"""
    from config.settings import ENGINE_STRATEGY, MAX_RECORDS_THRESHOLD

    # 如果指定了引擎类型，直接使用
    if ENGINE_STRATEGY != 'auto':
        if ENGINE_STRATEGY == 'pandas':
            from core.compare_engine.pandas_engine import PandasCompareEngine
            return PandasCompareEngine(config)
        elif ENGINE_STRATEGY in ['spark_local', 'spark_cluster']:
            from core.compare_engine.spark_engine import SparkCompareEngine
            return SparkCompareEngine(config, ENGINE_STRATEGY)

    # 自动选择：先获取数据量（使用连接池）
    adapter_config = {
        'db_type': config['src_db_type'],
        'host': config['src_host'],
        'port': config['src_port'],
        'user': config['src_username'],
        'password': config['src_password'],
        'database': config['src_db_name']
    }
    from utils.db_connection_pool import get_pooled_connection, return_pooled_connection
    adapter = get_pooled_connection(adapter_config)

    try:
        where_clause = ""
        if config.get('incremental') and config.get('update_time_str'):
            where_clause = BaseCompareEngine(config).get_where_clause()

        record_count = adapter.get_table_count(
            config['src_db_name'],
            config['src_table_name'],
            where_clause
        )
    finally:
        return_pooled_connection(adapter_config, adapter)

    # 根据数据量选择引擎
    if record_count < MAX_RECORDS_THRESHOLD:  # 50万以下用Pandas
        from core.compare_engine.pandas_engine import PandasCompareEngine
        return PandasCompareEngine(config)
    elif MAX_RECORDS_THRESHOLD <= record_count < MAX_RECORDS_THRESHOLD * 10:  # 50万-500万用Spark本地模式
        from core.compare_engine.spark_engine import SparkCompareEngine
        return SparkCompareEngine(config, 'spark_local')
    else:  # 500万以上用Spark集群模式
        from core.compare_engine.spark_engine import SparkCompareEngine
        return SparkCompareEngine(config, 'spark_cluster')
