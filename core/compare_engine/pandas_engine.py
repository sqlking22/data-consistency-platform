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
        """加载源端和目标端数据到Pandas DataFrame（支持分块加载）"""
        from config.settings import CHUNK_SIZE_FOR_DATA_SYNC
        import logging
        logger = logging.getLogger(__name__)

        # 获取比对字段
        columns = self.get_compare_columns()
        all_columns = columns['key_columns'] + columns['update_column'] + columns['extra_columns']
        all_columns = list(set(all_columns))  # 去重

        # 构建WHERE子句
        where_clause = self.get_where_clause()

        # 先获取总记录数，决定是否需要分块加载
        src_total_count = self.src_adapter.get_table_count(
            self.config['src_db_name'],
            self.config['src_table_name'],
            where_clause
        )
        tgt_total_count = self.tgt_adapter.get_table_count(
            self.config['tgt_db_name'],
            self.config['tgt_table_name'],
            where_clause
        )

        chunk_size = self.config.get('chunk_size_for_data_sync', CHUNK_SIZE_FOR_DATA_SYNC)

        # 如果数据量小于chunk_size，直接加载
        if src_total_count <= chunk_size and tgt_total_count <= chunk_size:
            logger.debug(f"数据量较小（源端{src_total_count}，目标端{tgt_total_count}），直接加载")

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
        else:
            # 数据量大，分块加载
            logger.info(f"数据量较大（源端{src_total_count}，目标端{tgt_total_count}），启用分块加载（每块{chunk_size}条）")

            # 分块加载源端数据
            src_chunks = []
            src_offset = 0
            while src_offset < src_total_count:
                chunk_data = self._query_data_with_limit(
                    self.src_adapter,
                    self.config['src_db_name'],
                    self.config['src_table_name'],
                    all_columns,
                    where_clause,
                    limit=chunk_size,
                    offset=src_offset
                )
                if not chunk_data:
                    break
                src_chunks.append(pd.DataFrame(chunk_data))
                src_offset += chunk_size
                logger.debug(f"已加载源端数据：{min(src_offset, src_total_count)}/{src_total_count}")

            self.src_df = pd.concat(src_chunks, ignore_index=True) if src_chunks else pd.DataFrame()
            self.compare_result['src_cnt'] = len(self.src_df)

            # 分块加载目标端数据
            tgt_chunks = []
            tgt_offset = 0
            while tgt_offset < tgt_total_count:
                chunk_data = self._query_data_with_limit(
                    self.tgt_adapter,
                    self.config['tgt_db_name'],
                    self.config['tgt_table_name'],
                    all_columns,
                    where_clause,
                    limit=chunk_size,
                    offset=tgt_offset
                )
                if not chunk_data:
                    break
                tgt_chunks.append(pd.DataFrame(chunk_data))
                tgt_offset += chunk_size
                logger.debug(f"已加载目标端数据：{min(tgt_offset, tgt_total_count)}/{tgt_total_count}")

            self.tgt_df = pd.concat(tgt_chunks, ignore_index=True) if tgt_chunks else pd.DataFrame()
            self.compare_result['tgt_cnt'] = len(self.tgt_df)

            logger.info(f"分块加载完成：源端{len(self.src_df)}条，目标端{len(self.tgt_df)}条")

        # 数据类型转换（统一类型）
        from utils.data_type_utils import unify_data_types
        self.src_df, self.tgt_df = unify_data_types(self.src_df, self.tgt_df)

    def _query_data_with_limit(self, adapter, db_name: str, table_name: str, columns: list,
                                where_clause: str = "", limit: int = None, offset: int = 0) -> list:
        """分页查询数据（支持LIMIT OFFSET）"""
        columns_str = ', '.join(columns)
        sql = f"SELECT {columns_str} FROM {db_name}.{table_name}"

        if where_clause:
            sql += f" WHERE {where_clause}"

        if limit:
            sql += f" LIMIT {limit} OFFSET {offset}"

        return adapter.query(sql)

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
            'mismatch_full': [], # 值不匹配的完整记录（包含源端和目标端所有字段）
            'src_only': [],      # 仅源端存在的记录
            'tgt_only': []       # 仅目标端存在的记录
        }

        # 获取不匹配记录（包含所有字段，用于后续时间字段比较）
        if mismatch_count > 0:
            mismatch_df = compare.all_mismatch()
            # 存储主键信息（用于向后兼容）
            diff_records['mismatch'] = mismatch_df[join_columns].to_dict('records')

            # 优化：使用set_index + loc实现O(1)查找，避免O(n²)复杂度
            # 预期性能提升：60倍以上
            update_column = columns.get('update_column', [])

            # 为源端和目标端DataFrame设置主键索引（如果尚未设置）
            if len(join_columns) == 1:
                # 单主键：直接设置索引
                pk_col = join_columns[0]
                src_indexed = self.src_df.set_index(pk_col, drop=False)
                tgt_indexed = self.tgt_df.set_index(pk_col, drop=False)
            else:
                # 联合主键：设置多级索引
                src_indexed = self.src_df.set_index(join_columns, drop=False)
                tgt_indexed = self.tgt_df.set_index(join_columns, drop=False)

            # 遍历mismatch记录，使用loc快速查找
            for _, row in mismatch_df.iterrows():
                # 构建主键条件
                pk_condition = {pk: row[pk] for pk in join_columns}

                # 使用索引快速查找（O(1)复杂度）
                try:
                    if len(join_columns) == 1:
                        # 单主键查找
                        pk_value = row[join_columns[0]]
                        src_record = src_indexed.loc[pk_value].to_dict() if pk_value in src_indexed.index else {}
                        tgt_record = tgt_indexed.loc[pk_value].to_dict() if pk_value in tgt_indexed.index else {}
                    else:
                        # 联合主键查找
                        pk_tuple = tuple(row[pk] for pk in join_columns)
                        src_record = src_indexed.loc[pk_tuple].to_dict() if pk_tuple in src_indexed.index else {}
                        tgt_record = tgt_indexed.loc[pk_tuple].to_dict() if pk_tuple in tgt_indexed.index else {}

                        # 处理可能的重复索引（联合主键可能返回多条记录）
                        if isinstance(src_record, dict) and 'index' in src_record:
                            # 如果返回多条，取第一条
                            if hasattr(src_record.get(list(src_record.keys())[0]), '__iter__'):
                                src_record = {k: v[0] if isinstance(v, list) else v for k, v in src_record.items()}
                        if isinstance(tgt_record, dict) and 'index' in tgt_record:
                            if hasattr(tgt_record.get(list(tgt_record.keys())[0]), '__iter__'):
                                tgt_record = {k: v[0] if isinstance(v, list) else v for k, v in tgt_record.items()}
                except (KeyError, IndexError) as e:
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.warning(f"查找mismatch记录失败: {pk_condition}, 错误: {str(e)}")
                    src_record = {}
                    tgt_record = {}

                # 存储包含源端和目标端完整数据的记录
                diff_records['mismatch_full'].append({
                    'pk': pk_condition,
                    'src_record': src_record,
                    'tgt_record': tgt_record,
                    'update_column': update_column
                })

        # 获取源端独有记录（包含所有字段）
        if src_only_count > 0:
            src_only_df = compare.df1_unq_rows
            diff_records['src_only'] = src_only_df.to_dict('records')

        # 获取目标端独有记录
        if tgt_only_count > 0:
            tgt_only_df = compare.df2_unq_rows
            diff_records['tgt_only'] = tgt_only_df[join_columns].to_dict('records')

        # 存储差异数据到compare_result
        self.compare_result['diff_records'] = diff_records
        import logging
        logger = logging.getLogger(__name__)
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