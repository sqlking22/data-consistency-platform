#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 12:44
# @Author  : hejun
import pandas as pd
import datacompy
from typing import Dict, List, Any
from datetime import datetime
from core.compare_engine.base_engine import BaseCompareEngine


class PandasCompareEngine(BaseCompareEngine):
    """Pandas比对引擎（适用于小数据量）"""

    def load_data(self):
        """加载源端和目标端数据到Pandas DataFrame"""
        # 获取比对字段
        columns = self.get_compare_columns()
        all_columns = columns['key_columns'] + columns['update_column'] + columns['extra_columns']
        all_columns = list(set(all_columns))  # 去重

        # 构建WHERE子句
        where_clause = self.get_where_clause()

        # 加载源端数据
        src_data = self.src_adapter.query_data(
            self.config['src_db_name'],
            self.config['src_table_name'],
            all_columns,
            where_clause
        )
        self.src_df = pd.DataFrame(src_data)
        self.compare_result['src_cnt'] = len(self.src_df)

        # 加载目标端数据
        tgt_data = self.tgt_adapter.query_data(
            self.config['tgt_db_name'],
            self.config['tgt_table_name'],
            all_columns,
            where_clause  # 目标端使用相同的过滤条件
        )
        self.tgt_df = pd.DataFrame(tgt_data)
        self.compare_result['tgt_cnt'] = len(self.tgt_df)

        # 数据类型转换（统一类型）
        from utils.data_type_utils import unify_data_types
        self.src_df, self.tgt_df = unify_data_types(self.src_df, self.tgt_df)

    def compare(self):
        """执行Pandas比对"""
        if self.src_df.empty and self.tgt_df.empty:
            self.compare_result['diff_cnt'] = 0
            self.compare_result['compare_report'] = "源端和目标端均无数据"
            self.compare_result['matching_rate'] = 1.0
            return

        # 获取主键列
        columns = self.get_compare_columns()
        join_columns = columns['key_columns']

        # 使用datacompy进行比对
        compare = datacompy.Compare(
            self.src_df,
            self.tgt_df,
            join_columns=join_columns,
            abs_tol=0,
            rel_tol=0,
            df1_name='Source',
            df2_name='Target'
        )

        # 记录比对结果
        # self.compare_result['diff_cnt'] = len(compare.all_mismatch()) + len(compare.df1_unq_columns()) + len(compare.df2_unq_columns())
        mismatch_count = len(compare.all_mismatch())
        src_only_count = len(compare.df1_unq_rows)
        tgt_only_count = len(compare.df2_unq_rows)
        self.compare_result['diff_cnt'] = mismatch_count + src_only_count + tgt_only_count

        # ===== 新增：捕获差异数据 =====
        diff_records = {
            'mismatch': [],      # 值不匹配的记录
            'src_only': [],      # 仅源端存在的记录
            'tgt_only': []       # 仅目标端存在的记录
        }

        # 获取不匹配记录
        if mismatch_count > 0:
            mismatch_df = compare.all_mismatch()
            diff_records['mismatch'] = mismatch_df[join_columns].to_dict('records')

        # 获取源端独有记录
        if src_only_count > 0:
            src_only_df = compare.df1_unq_rows
            diff_records['src_only'] = src_only_df[join_columns].to_dict('records')

        # 获取目标端独有记录
        if tgt_only_count > 0:
            tgt_only_df = compare.df2_unq_rows
            diff_records['tgt_only'] = tgt_only_df[join_columns].to_dict('records')

        # 存储差异数据到compare_result
        self.compare_result['diff_records'] = diff_records
        from utils.logger import logger
        logger.info(f"捕获到{sum(len(v) for v in diff_records.values())}条差异数据")
        # ===== 新增结束 =====

        self.compare_result['compare_report'] = compare.report()
        # 计算匹配的行数
        matched_rows = compare.count_matching_rows()
        total_records = max(self.compare_result['src_cnt'], self.compare_result['tgt_cnt'])
        if total_records > 0:
            self.compare_result['matching_rate'] = matched_rows / total_records
        else:
            self.compare_result['matching_rate'] = 1.0