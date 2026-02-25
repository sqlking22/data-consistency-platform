#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 16:16
# @Author  : hejun
import logging
from typing import Dict, List, Any
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when, lit
from core.compare_engine.base_engine import BaseCompareEngine

logger = logging.getLogger(__name__)


class SparkCompareEngine(BaseCompareEngine):
    """Spark比对引擎（适用于大数据量）"""

    def __init__(self, config: Dict[str, Any], spark_mode: str = 'spark_local'):
        super().__init__(config)
        self.spark_mode = spark_mode
        self.spark: SparkSession = None
        self.src_spark_df: DataFrame = None
        self.tgt_spark_df: DataFrame = None

    def init_spark(self):
        """初始化Spark会话"""
        builder = SparkSession.builder.appName(f"DataCompare_{self.config['src_table_name']}")

        if self.spark_mode == 'spark_local':
            builder = builder.master("local[*]") \
                .config("spark.driver.memory", "8g") \
                .config("spark.executor.memory", "4g")
        # Spark集群模式使用集群配置，无需指定master

        self.spark = builder.getOrCreate()
        logger.info(f"Spark会话初始化完成（模式：{self.spark_mode}）")

    def load_data(self):
        """加载源端和目标端数据到Spark DataFrame"""
        self.init_spark()

        # 获取比对字段
        columns = self.get_compare_columns()
        all_columns = columns['key_columns'] + columns['update_column'] + columns['extra_columns']
        all_columns = list(set(all_columns))

        # 构建WHERE子句
        where_clause = self.get_where_clause()

        # 加载源端数据
        self.src_spark_df = self._load_spark_data('src', all_columns, where_clause)
        self.compare_result['src_cnt'] = self.src_spark_df.count()

        # 加载目标端数据
        self.tgt_spark_df = self._load_spark_data('tgt', all_columns, where_clause)
        self.compare_result['tgt_cnt'] = self.tgt_spark_df.count()

    def _load_spark_data(self, db_side: str, columns: List[str], where_clause: str) -> DataFrame:
        """加载Spark数据"""
        config_prefix = f"{db_side}_"
        db_type = self.config[f"{config_prefix}db_type"].lower()
        jdbc_url = self._get_jdbc_url(db_side)
        table_name = self.config[f"{config_prefix}table_name"]
        db_name = self.config[f"{config_prefix}db_name"]

        # 构建JDBC配置
        jdbc_config = {
            "url": jdbc_url,
            "dbtable": f"{db_name}.{table_name}",
            "user": self.config[f"{config_prefix}username"],
            "password": self.config[f"{config_prefix}password"],
            "driver": self._get_jdbc_driver(db_type)
        }

        # 加载数据
        df = self.spark.read.jdbc(**jdbc_config)

        # 过滤列和WHERE条件
        df = df.select([col(c) for c in columns if c in df.columns])
        if where_clause:
            df = df.filter(where_clause)

        return df

    def _get_jdbc_driver(self, db_type: str) -> str:
        """获取JDBC驱动类名"""
        drivers = {
            'mysql': 'com.mysql.cj.jdbc.Driver',
            'oracle': 'oracle.jdbc.driver.OracleDriver',
            'sqlserver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            'postgresql': 'org.postgresql.Driver'
        }
        return drivers.get(db_type, drivers['mysql'])

    def _get_jdbc_url(self, db_side: str) -> str:
        """生成JDBC URL"""
        config_prefix = f"{db_side}_"
        db_type = self.config[f"{config_prefix}db_type"].lower()
        host = self.config[f"{config_prefix}host"]
        port = self.config[f"{config_prefix}port"]
        db_name = self.config[f"{config_prefix}db_name"]

        if db_type == 'mysql':
            return f"jdbc:mysql://{host}:{port}/{db_name}?useUnicode=true&characterEncoding=utf8"
        elif db_type == 'oracle':
            return f"jdbc:oracle:thin:@{host}:{port}:{db_name}"
        elif db_type == 'sqlserver':
            return f"jdbc:sqlserver://{host}:{port};DatabaseName={db_name}"
        elif db_type == 'postgresql':
            return f"jdbc:postgresql://{host}:{port}/{db_name}"

        return ""

    def compare(self):
        """执行Spark比对"""
        if self.src_spark_df.isEmpty() and self.tgt_spark_df.isEmpty():
            self.compare_result['diff_cnt'] = 0
            self.compare_result['compare_report'] = "源端和目标端均无数据"
            self.compare_result['matching_rate'] = 1.0
            return

        # 获取主键列
        columns = self.get_compare_columns()
        join_columns = columns['key_columns']

        # 全外连接比对
        joined_df = self.src_spark_df.alias('src').join(
            self.tgt_spark_df.alias('tgt'),
            on=join_columns,
            how='full_outer'
        )

        # 标记差异行
        diff_conditions = []
        for col_name in self.src_spark_df.columns:
            if col_name not in join_columns and col_name in self.tgt_spark_df.columns:
                diff_conditions.append(
                    when(~col(f"src.{col_name}").eqNullSafe(col(f"tgt.{col_name}")), 1).otherwise(0)
                )

        # 计算差异记录数
        if diff_conditions:
            # 对所有差异条件求和，如果总和大于0，则表示该行存在差异
            total_diff = sum(diff_conditions)
            diff_df = joined_df.withColumn(
                "is_diff",
                when(total_diff > 0, True).otherwise(False)
            )
            diff_count = diff_df.filter(col("is_diff")).count()
        else:
            # 仅主键比对 - 找到只存在于一边的记录
            diff_count = joined_df.filter(
                col(f"src.{join_columns[0]}").isNull() | col(f"tgt.{join_columns[0]}").isNull()
            ).count()

        # 记录比对结果
        self.compare_result['diff_cnt'] = diff_count
        self.compare_result[
            'compare_report'] = f"Spark比对完成：源端{self.compare_result['src_cnt']}条，目标端{self.compare_result['tgt_cnt']}条，差异{diff_count}条"

        # 计算匹配率
        total_records = max(self.compare_result['src_cnt'], self.compare_result['tgt_cnt'])
        if total_records > 0:
            self.compare_result['matching_rate'] = (total_records - diff_count) / total_records
        else:
            self.compare_result['matching_rate'] = 1.0

    def run(self) -> Dict[str, Any]:
        """执行完整比对流程（重写以关闭Spark）"""
        try:
            result = super().run()
            return result
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("Spark会话已关闭")