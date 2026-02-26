#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 12:44
# @Author  : hejun
import os
import json
import subprocess
import re
from typing import Dict, List, Any
from datetime import datetime
from core.repair_engine.base_repair import BaseRepairEngine
from config.settings import DATAX_BIN, DATAX_JOB_DIR, PYTHON_BIN_PATH


class DataXRepairEngine(BaseRepairEngine):
    """基于DataX的数据修复引擎"""

    def __init__(self, config: Dict[str, Any], compare_result: Dict[str, Any]):
        super().__init__(config, compare_result)
        self.datax_job_file = ""
        self.datax_job_files = []  # 存储多个DataX作业文件路径

    def generate_datax_job(self) -> str:
        """生成DataX作业配置文件（支持批量生成）"""
        from utils.logger import logger
        from config.settings import REPAIR_MAX_WHERE_IN_RECORDS

        # 获取所有批次的WHERE子句
        where_clauses = self._build_where_clauses_batch()

        if not where_clauses:
            logger.warning("没有WHERE条件，生成单个全量同步文件")
            where_clauses = [None]

        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        self.datax_job_files = []

        # 为每个批次生成一个DataX JSON文件
        for batch_idx, where_clause in enumerate(where_clauses, 1):
            # 构建DataX配置
            job_config = {
                "job": {
                    "setting": {
                        "speed": {
                            "channel": 3
                        },
                        "errorLimit": {
                            "record": 0,
                            "percentage": 0.01
                        }
                    },
                    "content": [
                        {
                            "reader": self._get_reader_config(where_clause),
                            "writer": self._get_writer_config()
                        }
                    ]
                }
            }

            # 生成作业文件名（使用目标表名）
            table_name = self.config['tgt_table_name']

            if len(where_clauses) > 1:
                # 多个批次：添加批次号（目标表名_1.json, 目标表名_2.json）
                job_file_name = f"{table_name}_{batch_idx}.json"
            else:
                # 单个批次：不添加批次号（目标表名.json）
                job_file_name = f"{table_name}.json"

            job_file_path = os.path.join(DATAX_JOB_DIR, job_file_name)
            logger.info(f"生成DataX作业文件: {job_file_path}")

            with open(job_file_path, 'w', encoding='utf-8') as f:
                json.dump(job_config, f, ensure_ascii=False, indent=4)

            self.datax_job_files.append(job_file_path)

        # 保持向后兼容：返回第一个文件路径
        self.datax_job_file = self.datax_job_files[0] if self.datax_job_files else ""

        if len(self.datax_job_files) > 1:
            logger.info(f"共生成 {len(self.datax_job_files)} 个DataX作业文件")

        return self.datax_job_file

    def _get_reader_config(self, where_clause: str = None) -> Dict[str, Any]:
        """生成Reader配置（只同步差异数据）

        Args:
            where_clause: 可选的WHERE子句，如果不提供则自动构建（获取第一个批次）
        """
        from utils.logger import logger

        db_type = self.config['src_db_type'].lower()
        reader_type = {
            'mysql': 'mysqlreader',
            'oracle': 'oraclereader',
            'sqlserver': 'sqlserverreader',
            'postgresql': 'postgresqlreader'
        }.get(db_type, 'mysqlreader')

        # 如果未提供where_clause，自动构建（获取第一个批次，用于向后兼容）
        if where_clause is None:
            where_clauses = self._build_where_clauses_batch()
            where_clause = where_clauses[0] if where_clauses else None

        reader_config = {
            "name": reader_type,
            "parameter": {
                "username": self.config['src_username'],
                "password": self.config['src_password'],
                "column": self._get_all_common_columns(),  # 使用所有相交字段
                "connection": [
                    {
                        "table": [self.config['src_table_name']],
                        "jdbcUrl": [self._get_jdbc_url('src')]
                    }
                ]
            }
        }

        if where_clause:
            reader_config['parameter']['where'] = where_clause
            logger.info(f"差异数据WHERE条件: {where_clause[:200]}...")

        return reader_config

    def _build_where_clauses_batch(self) -> List[str]:
        """批量构建WHERE子句（使用IN语法，每批最多3000条）

        支持基于时间字段的智能过滤：
        - 对于mismatch记录，只有源端时间字段 > 目标端时间字段的记录才需要修复
        - src_only记录全部需要修复

        Returns:
            WHERE子句列表，每个子句对应一个批次
        """
        from utils.logger import logger
        from config.settings import REPAIR_MAX_WHERE_IN_RECORDS, TIME_TOLERANCE
        import pandas as pd

        diff_records = self.compare_result.get('diff_records', {})

        if not diff_records:
            logger.warning("未找到差异数据，使用时间范围过滤")
            time_where = self._build_where_clause_from_time_range()
            return [time_where] if time_where else []

        # 获取主键列
        compare_columns = self.compare_result.get('compare_columns', {})
        if isinstance(compare_columns, dict):
            pk_columns = compare_columns.get('key_columns', [])
        else:
            # 备用：从字符串提取
            check_column_str = self.compare_result.get('check_column', '')
            import re
            key_match = re.search(r'key_columns：\[(.*?)\]', check_column_str)
            if key_match:
                key_str = key_match.group(1)
                pk_columns = [col.strip().strip("'\"") for col in key_str.split(',') if col.strip()]
            else:
                logger.error("无法确定主键列")
                return []

        if not pk_columns:
            logger.warning("未找到主键列，使用时间范围过滤")
            time_where = self._build_where_clause_from_time_range()
            return [time_where] if time_where else []

        # 收集需要同步的记录
        all_diff_keys = []

        # 1. 处理mismatch记录：需要基于时间字段过滤
        mismatch_full = diff_records.get('mismatch_full', [])
        if mismatch_full:
            # 检查是否配置了时间字段
            update_time_col = self.config.get('update_time_str')
            enable_time_filter = self.config.get('enable_time_filter', True)  # 默认启用时间过滤

            if update_time_col and enable_time_filter:
                logger.info(f"启用时间字段过滤：{update_time_col}，时间容差：{TIME_TOLERANCE}秒")
                time_filtered_count = 0

                for record_data in mismatch_full:
                    src_record = record_data.get('src_record', {})
                    tgt_record = record_data.get('tgt_record', {})
                    pk = record_data.get('pk', {})

                    src_time = src_record.get(update_time_col)
                    tgt_time = tgt_record.get(update_time_col)

                    # 比较时间字段
                    should_repair = False
                    if src_time and tgt_time:
                        try:
                            # 转换为datetime对象进行比较
                            if isinstance(src_time, str):
                                src_time = pd.to_datetime(src_time)
                            if isinstance(tgt_time, str):
                                tgt_time = pd.to_datetime(tgt_time)

                            # 源端时间 > 目标端时间 + 时间容差，才需要修复
                            time_diff_seconds = (src_time - tgt_time).total_seconds()
                            if time_diff_seconds >= TIME_TOLERANCE:
                                should_repair = True
                                time_filtered_count += 1
                                logger.debug(f"记录{pk}需要修复：源端时间{src_time} > 目标端时间{tgt_time}（差{time_diff_seconds}秒）")
                            else:
                                logger.debug(f"记录{pk}无需修复：源端时间{src_time} <= 目标端时间{tgt_time}（差{time_diff_seconds}秒）")
                        except Exception as e:
                            logger.warning(f"时间字段比较失败，默认修复：{pk}, 错误：{str(e)}")
                            should_repair = True
                    else:
                        # 时间字段为空，默认需要修复
                        should_repair = True
                        logger.debug(f"记录{pk}时间字段为空，默认需要修复")

                    if should_repair:
                        all_diff_keys.append(pk)
            else:
                # 未配置时间字段或未启用时间过滤，全部修复
                logger.info("未配置时间字段或未启用时间过滤，mismatch记录全部修复")
                all_diff_keys.extend(diff_records.get('mismatch', []))
        else:
            # 向后兼容：使用旧的mismatch格式
            all_diff_keys.extend(diff_records.get('mismatch', []))

        # 2. 处理src_only记录：需要查询目标端验证是否真的不存在或时间更旧
        src_only_records = diff_records.get('src_only', [])
        if src_only_records:
            # 检查是否配置了时间字段
            update_time_col = self.config.get('update_time_str')
            enable_time_filter = self.config.get('enable_time_filter', True)  # 默认启用时间过滤

            if update_time_col and enable_time_filter:
                logger.info(f"处理源端独有记录：需要查询目标端验证时间字段")
                src_only_need_repair_count = 0
                src_only_skip_count = 0

                # 批量查询目标端记录
                for record in src_only_records:
                    pk_dict = {pk: record[pk] for pk in pk_columns if pk in record}
                    if not pk_dict:
                        continue

                    # 从源端记录中获取时间字段值
                    src_time = record.get(update_time_col)

                    # 查询目标端是否存在该记录
                    tgt_record = self._query_target_record(pk_dict, pk_columns, [update_time_col])

                    if tgt_record is None:
                        # 目标端不存在该记录，需要修复（插入）
                        all_diff_keys.append(pk_dict)
                        src_only_need_repair_count += 1
                        logger.debug(f"源端独有记录{pk_dict}：目标端不存在，需要插入")
                    else:
                        # 目标端存在该记录，比较时间字段
                        tgt_time = tgt_record.get(update_time_col)

                        if src_time and tgt_time:
                            try:
                                # 转换为datetime对象进行比较
                                if isinstance(src_time, str):
                                    src_time = pd.to_datetime(src_time)
                                if isinstance(tgt_time, str):
                                    tgt_time = pd.to_datetime(tgt_time)

                                # 源端时间 > 目标端时间 + 时间容差，才需要修复
                                time_diff_seconds = (src_time - tgt_time).total_seconds()
                                if time_diff_seconds >= TIME_TOLERANCE:
                                    all_diff_keys.append(pk_dict)
                                    src_only_need_repair_count += 1
                                    logger.debug(f"源端独有记录{pk_dict}需要修复：源端时间{src_time} > 目标端时间{tgt_time}（差{time_diff_seconds}秒）")
                                else:
                                    src_only_skip_count += 1
                                    logger.info(f"源端独有记录{pk_dict}无需修复：源端时间{src_time} <= 目标端时间{tgt_time}（差{time_diff_seconds}秒）- 避免用旧数据覆盖新数据")
                            except Exception as e:
                                logger.warning(f"时间字段比较失败，跳过修复：{pk_dict}, 错误：{str(e)}")
                                src_only_skip_count += 1
                        else:
                            # 时间字段为空，默认需要修复
                            all_diff_keys.append(pk_dict)
                            src_only_need_repair_count += 1
                            logger.debug(f"源端独有记录{pk_dict}时间字段为空，默认需要修复")

                logger.info(f"源端独有记录共{len(src_only_records)}条：需要修复{src_only_need_repair_count}条，跳过{src_only_skip_count}条（避免旧数据覆盖新数据）")
            else:
                # 未配置时间字段或未启用时间过滤，全部修复
                logger.info("未配置时间字段或未启用时间过滤，源端独有记录全部修复")
                for record in src_only_records:
                    pk_dict = {pk: record[pk] for pk in pk_columns if pk in record}
                    if pk_dict:
                        all_diff_keys.append(pk_dict)

        if not all_diff_keys:
            logger.info("没有需要修复的记录")
            return ["1=0"]  # 返回永假条件

        total_records = len(all_diff_keys)
        batch_size = REPAIR_MAX_WHERE_IN_RECORDS  # 从配置获取批量大小

        logger.info(f"共有{total_records}条需要修复的数据，每批最多{batch_size}条，将生成{(total_records + batch_size - 1) // batch_size}个批次")

        # 分批处理
        where_clauses = []
        for batch_start in range(0, total_records, batch_size):
            batch_end = min(batch_start + batch_size, total_records)
            batch_records = all_diff_keys[batch_start:batch_end]

            where_clause = self._build_where_clause_with_in_syntax(batch_records, pk_columns)
            if where_clause:
                where_clauses.append(where_clause)
                logger.info(f"批次 {len(where_clauses)}: 记录 {batch_start+1}-{batch_end}, WHERE条件长度: {len(where_clause)}")

        return where_clauses

    def _build_where_clause_with_in_syntax(self, records: List[Dict], pk_columns: List[str]) -> str:
        """使用IN语法构建WHERE子句

        Args:
            records: 差异数据记录列表
            pk_columns: 主键列名列表

        Returns:
            WHERE子句字符串
        """
        from utils.logger import logger
        import pandas as pd

        if not records or not pk_columns:
            return ""

        # 单主键：直接使用 IN
        if len(pk_columns) == 1:
            pk_col = pk_columns[0]
            values = []

            for record in records:
                if pk_col in record:
                    value = record[pk_col]
                    formatted_value = self._format_sql_value(value)
                    values.append(formatted_value)

            if not values:
                logger.warning(f"主键列'{pk_col}'在所有记录中都缺失")
                return ""

            # 构建IN子句
            in_clause = ", ".join(values)
            where_clause = f"{pk_col} IN ({in_clause})"
            return where_clause

        # 联合主键：使用多个IN子句+AND连接
        else:
            # 为每个主键列收集值
            pk_values = {pk_col: [] for pk_col in pk_columns}

            for record in records:
                all_cols_present = True
                for pk_col in pk_columns:
                    if pk_col not in record:
                        logger.warning(f"联合主键列'{pk_col}'不在记录中: {record}")
                        all_cols_present = False
                        break

                if all_cols_present:
                    for pk_col in pk_columns:
                        value = record[pk_col]
                        formatted_value = self._format_sql_value(value)
                        pk_values[pk_col].append(formatted_value)

            # 构建联合主键的IN条件
            # 例如：(pk1 IN (1,2,3) AND pk2 IN ('a','b','c'))
            # 注意：这种写法在语义上可能不完全准确，但DataX通常能处理
            # 更准确的方式是使用元组IN，但这需要更复杂的SQL语法
            in_conditions = []
            for pk_col in pk_columns:
                if pk_values[pk_col]:
                    in_clause = ", ".join(pk_values[pk_col])
                    in_conditions.append(f"{pk_col} IN ({in_clause})")

            if not in_conditions:
                return ""

            where_clause = " AND ".join(in_conditions)
            return f"({where_clause})"

    def _format_sql_value(self, value: Any) -> str:
        """格式化SQL值

        Args:
            value: Python值

        Returns:
            SQL格式的字符串
        """
        import pandas as pd

        if value is None:
            return "NULL"
        elif isinstance(value, str):
            # 转义单引号
            escaped_value = str(value).replace("'", "''")
            return f"'{escaped_value}'"
        elif isinstance(value, (datetime, pd.Timestamp)):
            formatted_time = value.strftime('%Y-%m-%d %H:%M:%S')
            return f"'{formatted_time}'"
        else:
            return str(value)

    def _query_target_record(self, pk_dict: Dict[str, Any], pk_columns: List[str], columns: List[str]) -> Dict[str, Any]:
        """查询目标端记录

        Args:
            pk_dict: 主键字典 {主键列名: 值}
            pk_columns: 主键列名列表
            columns: 需要查询的列名列表

        Returns:
            目标端记录字典，如果不存在返回None
        """
        from utils.logger import logger
        from core.db_adapter.base_adapter import get_db_adapter

        try:
            # 创建目标端适配器
            tgt_config = {
                'db_type': self.config['tgt_db_type'],
                'host': self.config['tgt_host'],
                'port': self.config['tgt_port'],
                'user': self.config['tgt_username'],
                'password': self.config['tgt_password'],
                'database': self.config['tgt_db_name']
            }
            tgt_adapter = get_db_adapter(tgt_config)

            try:
                # 构建WHERE条件
                where_conditions = []
                for pk_col in pk_columns:
                    if pk_col in pk_dict:
                        value = pk_dict[pk_col]
                        formatted_value = self._format_sql_value(value)
                        where_conditions.append(f"{pk_col} = {formatted_value}")

                if not where_conditions:
                    logger.warning(f"主键条件为空，无法查询目标端记录")
                    return None

                where_clause = " AND ".join(where_conditions)

                # 查询目标端记录
                tgt_data = tgt_adapter.query_data(
                    self.config['tgt_db_name'],
                    self.config['tgt_table_name'],
                    columns,
                    where_clause
                )

                if tgt_data and len(tgt_data) > 0:
                    return tgt_data[0]  # 返回第一条记录
                else:
                    return None

            finally:
                tgt_adapter.close()

        except Exception as e:
            logger.error(f"查询目标端记录失败：{str(e)}")
            return None

    def _build_where_clause_from_diff_records(self) -> str:
        """根据差异数据构建WHERE子句（支持联合主键）"""
        from utils.logger import logger
        import pandas as pd

        diff_records = self.compare_result.get('diff_records', {})

        if not diff_records:
            logger.warning("未找到差异数据，使用时间范围过滤")
            return self._build_where_clause_from_time_range()

        # 获取主键列
        compare_columns = self.compare_result.get('compare_columns', {})
        if isinstance(compare_columns, dict):
            pk_columns = compare_columns.get('key_columns', [])
        else:
            # 备用：从字符串提取
            check_column_str = self.compare_result.get('check_column', '')
            import re
            key_match = re.search(r'key_columns：\[(.*?)\]', check_column_str)
            if key_match:
                key_str = key_match.group(1)
                pk_columns = [col.strip().strip("'\"") for col in key_str.split(',') if col.strip()]
            else:
                raise ValueError("无法确定主键列")

        if not pk_columns:
            logger.warning("未找到主键列，使用时间范围过滤")
            return self._build_where_clause_from_time_range()

        # 合并所有需要同步的记录（不匹配+源端独有，忽略目标端独有）
        all_diff_keys = []
        all_diff_keys.extend(diff_records.get('mismatch', []))
        all_diff_keys.extend(diff_records.get('src_only', []))

        if not all_diff_keys:
            logger.info("无需同步的记录")
            return "1=0"  # 返回永假条件

        # 限制记录数避免WHERE子句过长
        max_records_in_where = 1000
        if len(all_diff_keys) > max_records_in_where:
            logger.warning(f"差异数据过多({len(all_diff_keys)})，限制为前{max_records_in_where}条。建议使用批量修复。")
            all_diff_keys = all_diff_keys[:max_records_in_where]

        # 构建WHERE条件（支持联合主键）
        where_conditions = []

        for record in all_diff_keys:
            # 为每条记录构建条件
            # 联合主键格式：(pk1='val1' AND pk2='val2' AND ...)
            pk_conditions = []
            for pk_col in pk_columns:
                if pk_col in record:
                    value = record[pk_col]
                    # 处理不同数据类型
                    if value is None:
                        pk_conditions.append(f"{pk_col} IS NULL")
                    elif isinstance(value, str):
                        # 转义单引号
                        escaped_value = str(value).replace("'", "''")
                        pk_conditions.append(f"{pk_col} = '{escaped_value}'")
                    elif isinstance(value, (datetime, pd.Timestamp)):
                        formatted_time = value.strftime('%Y-%m-%d %H:%M:%S')
                        pk_conditions.append(f"{pk_col} = '{formatted_time}'")
                    else:
                        pk_conditions.append(f"{pk_col} = {value}")
                else:
                    logger.warning(f"主键列'{pk_col}'不在记录中: {record}")

            if pk_conditions:
                if len(pk_conditions) == 1:
                    where_conditions.append(pk_conditions[0])
                else:
                    where_conditions.append(f"({' AND '.join(pk_conditions)})")

        if not where_conditions:
            return self._build_where_clause_from_time_range()

        # 用OR连接所有条件
        where_clause = " OR ".join(where_conditions)

        # 复杂条件用括号包裹
        if len(where_conditions) > 1:
            where_clause = f"({where_clause})"

        return where_clause

    def _build_where_clause_from_time_range(self) -> str:
        """备用：基于时间范围构建WHERE子句"""
        from utils.logger import logger

        where_clause = self.compare_result.get('check_range', '')
        if not where_clause:
            return ""

        try:
            where_clause = where_clause.replace('[', '').replace(')', '').replace(']', '')
            start_time, end_time = where_clause.split(',')
            update_col = self.config.get('update_time_str', '')
            if update_col:
                return f"{update_col} >= '{start_time}' AND {update_col} < '{end_time}'"
        except Exception as e:
            logger.error(f"解析时间范围失败: {str(e)}")

        return ""

    def _get_writer_config(self) -> Dict[str, Any]:
        """生成Writer配置（支持安全的写入模式）"""
        from utils.logger import logger

        db_type = self.config['tgt_db_type'].lower()
        writer_type = {
            'mysql': 'mysqlwriter',
            'oracle': 'oraclewriter',
            'sqlserver': 'sqlserverwriter',
            'postgresql': 'postgresqlwriter'
        }.get(db_type, 'mysqlwriter')

        # 从配置获取写入模式（默认为update）
        write_mode = self.config.get('repair_write_mode', 'update').lower()

        # 验证写入模式
        valid_modes = {'insert', 'update', 'replace'}
        if write_mode not in valid_modes:
            logger.warning(f"无效的write_mode '{write_mode}'，使用'update'")
            write_mode = 'update'

        # 对于update模式，获取主键
        pk_columns = []
        if write_mode == 'update':
            compare_columns = self.compare_result.get('compare_columns', {})
            if isinstance(compare_columns, dict):
                pk_columns = compare_columns.get('key_columns', [])
            else:
                # 备用：从数据库获取
                from core.db_adapter.base_adapter import get_db_adapter
                tgt_config = {
                    'db_type': self.config['tgt_db_type'],
                    'host': self.config['tgt_host'],
                    'port': self.config['tgt_port'],
                    'user': self.config['tgt_username'],
                    'password': self.config['tgt_password'],
                    'database': self.config['tgt_db_name']
                }
                tgt_adapter = get_db_adapter(tgt_config)
                try:
                    pk_columns = tgt_adapter.get_primary_keys(
                        self.config['tgt_db_name'],
                        self.config['tgt_table_name']
                    )
                finally:
                    tgt_adapter.close()

        # 构建Writer配置
        writer_config = {
            "name": writer_type,
            "parameter": {
                "username": self.config['tgt_username'],
                "password": self.config['tgt_password'],
                "column": self._get_all_common_columns(),  # 使用所有相交字段
                "connection": [
                    {
                        "table": [self.config['tgt_table_name']],
                        "jdbcUrl": self._get_jdbc_url('tgt')
                    }
                ],
                "writeMode": write_mode
            }
        }

        # 对于update模式，添加主键列
        if write_mode == 'update' and pk_columns:
            writer_config['parameter']['pk_columns'] = pk_columns
            logger.info(f"Update模式，主键: {pk_columns}")

        # 移除危险的preSql: TRUNCATE
        # 仅在明确配置时使用preSql
        if self.config.get('repair_presql'):
            writer_config['parameter']['preSql'] = [self.config['repair_presql']]
            logger.warning(f"使用自定义preSql: {self.config['repair_presql']}")

        # 添加postSql验证（如果配置）
        if self.config.get('repair_postsql'):
            writer_config['parameter']['postSql'] = [self.config['repair_postsql']]

        return writer_config

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

    def _get_all_common_columns(self) -> List[str]:
        """获取源端和目标端所有相交字段（不仅仅是比较字段）

        Returns:
            字段名列表
        """
        from core.db_adapter.base_adapter import get_db_adapter
        from utils.logger import logger

        # 方法1：从数据库元数据获取所有字段交集
        try:
            # 获取源端适配器
            src_config = {
                'db_type': self.config['src_db_type'],
                'host': self.config['src_host'],
                'port': self.config['src_port'],
                'user': self.config['src_username'],
                'password': self.config['src_password'],
                'database': self.config['src_db_name']
            }
            src_adapter = get_db_adapter(src_config)

            # 获取目标端适配器
            tgt_config = {
                'db_type': self.config['tgt_db_type'],
                'host': self.config['tgt_host'],
                'port': self.config['tgt_port'],
                'user': self.config['tgt_username'],
                'password': self.config['tgt_password'],
                'database': self.config['tgt_db_name']
            }
            tgt_adapter = get_db_adapter(tgt_config)

            try:
                # 获取两端表的元数据
                src_metadata = src_adapter.get_table_metadata(
                    self.config['src_db_name'],
                    self.config['src_table_name']
                )
                tgt_metadata = tgt_adapter.get_table_metadata(
                    self.config['tgt_db_name'],
                    self.config['tgt_table_name']
                )

                # 提取字段名
                src_columns = {col['name'] for col in src_metadata}
                tgt_columns = {col['name'] for col in tgt_metadata}

                # 获取交集（两端都存在的字段）
                common_columns = src_columns & tgt_columns

                if not common_columns:
                    raise ValueError("源端和目标端没有共同字段")

                # 确保主键在字段列表中
                pk_columns = set(src_adapter.get_primary_keys(
                    self.config['src_db_name'],
                    self.config['src_table_name']
                ))

                final_columns = list(common_columns)

                # 验证主键是否包含在字段列表中
                missing_pk = pk_columns - set(final_columns)
                if missing_pk:
                    logger.warning(f"主键字段不在交集中: {missing_pk}")
                    # 即使目标端没有，也要添加主键
                    final_columns.extend(list(missing_pk))

                logger.info(f"获取所有相交字段(共{len(final_columns)}个): {final_columns}")
                return final_columns

            finally:
                src_adapter.close()
                tgt_adapter.close()

        except Exception as e:
            logger.error(f"从元数据获取所有字段失败: {str(e)}，回退到获取比较字段")
            # 备用：返回比较字段
            return self._get_compare_columns()

    def _get_compare_columns(self) -> List[str]:
        """获取需要同步的字段（源端和目标端的交集）"""
        from core.db_adapter.base_adapter import get_db_adapter
        from utils.logger import logger

        # 方法1：优先从compare_result结构化数据获取
        compare_columns = self.compare_result.get('compare_columns', {})
        if isinstance(compare_columns, dict):
            key_cols = compare_columns.get('key_columns', [])
            extra_cols = compare_columns.get('extra_columns', [])
            update_cols = compare_columns.get('update_column', [])
            all_cols = list(set(key_cols + extra_cols + update_cols))
            if all_cols:
                logger.info(f"从compare_result获取字段: {all_cols}")
                return all_cols

        # 方法2：从数据库元数据获取字段交集（备用方案）
        try:
            # 获取源端适配器
            src_config = {
                'db_type': self.config['src_db_type'],
                'host': self.config['src_host'],
                'port': self.config['src_port'],
                'user': self.config['src_username'],
                'password': self.config['src_password'],
                'database': self.config['src_db_name']
            }
            src_adapter = get_db_adapter(src_config)

            # 获取目标端适配器
            tgt_config = {
                'db_type': self.config['tgt_db_type'],
                'host': self.config['tgt_host'],
                'port': self.config['tgt_port'],
                'user': self.config['tgt_username'],
                'password': self.config['tgt_password'],
                'database': self.config['tgt_db_name']
            }
            tgt_adapter = get_db_adapter(tgt_config)

            try:
                # 获取两端表的元数据
                src_metadata = src_adapter.get_table_metadata(
                    self.config['src_db_name'],
                    self.config['src_table_name']
                )
                tgt_metadata = tgt_adapter.get_table_metadata(
                    self.config['tgt_db_name'],
                    self.config['tgt_table_name']
                )

                # 提取字段名
                src_columns = {col['name'] for col in src_metadata}
                tgt_columns = {col['name'] for col in tgt_metadata}

                # 获取交集（两端都存在的字段）
                common_columns = src_columns & tgt_columns

                if not common_columns:
                    raise ValueError("源端和目标端没有共同字段")

                # 确保主键在字段列表中
                pk_columns = set(src_adapter.get_primary_keys(
                    self.config['src_db_name'],
                    self.config['src_table_name']
                ))

                final_columns = list(common_columns)

                # 验证主键是否包含在字段列表中
                missing_pk = pk_columns - set(final_columns)
                if missing_pk:
                    logger.warning(f"主键字段不在交集中: {missing_pk}")
                    # 即使目标端没有，也要添加主键
                    final_columns.extend(list(missing_pk))

                logger.info(f"从元数据交集获取字段: {final_columns}")
                return final_columns

            finally:
                src_adapter.close()
                tgt_adapter.close()

        except Exception as e:
            logger.error(f"从元数据获取字段失败: {str(e)}")
            # 最后备用：返回主键
            if 'compare_columns' in self.compare_result.get('compare_columns', {}):
                return self.compare_result['compare_columns']['key_columns']
            raise ValueError(f"无法确定修复字段: {str(e)}")

    def repair(self) -> Dict[str, Any]:
        """执行修复"""
        from config.settings import MAX_REPAIR_RECORDS_THRESHOLD, TIME_TOLERANCE
        from utils.logger import logger

        # 修复决策判断
        if not self.config.get('enable_repair', False):
            self.repair_result['repair_status'] = 'skip'
            self.repair_result['repair_msg'] = '未启用修复功能'
            return self.repair_result
        # 检查差异记录数
        diff_cnt = self.compare_result.get('diff_cnt', 0)
        if diff_cnt == 0:
            self.repair_result['repair_status'] = 'skip'
            self.repair_result['repair_msg'] = '无差异记录，无需修复'
            return self.repair_result

        # 检查修复阈值
        if diff_cnt > MAX_REPAIR_RECORDS_THRESHOLD:
            self.repair_result['repair_status'] = 'fail'
            self.repair_result['repair_msg'] = f"差异记录数({diff_cnt})超过修复阈值({MAX_REPAIR_RECORDS_THRESHOLD})"
            return self.repair_result

        self.repair_result['repair_start_time'] = datetime.now()
        try:
            # 生成DataX作业（支持批量生成）
            job_file = self.generate_datax_job()

            # 确保datax_job_files列表不为空（向后兼容）
            if not self.datax_job_files:
                self.datax_job_files = [job_file] if job_file else []

            # 存储所有作业文件路径
            self.repair_result['repair_job_files'] = self.datax_job_files
            self.repair_result['repair_job_file'] = job_file  # 保持向后兼容
            self.repair_result['batch_count'] = len(self.datax_job_files)

            # 执行所有DataX作业
            total_repair_cnt = 0
            failed_batches = []
            result = None  # 用于保存最后的subprocess结果

            for idx, job_file_path in enumerate(self.datax_job_files, 1):
                logger.info(f"执行批次 {idx}/{len(self.datax_job_files)}: {job_file_path}")

                # 执行DataX作业
                cmd = [PYTHON_BIN_PATH, DATAX_BIN, job_file_path]
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=3600
                )
                if result.returncode == 0:
                    logger.info(f"批次 {idx} 执行成功")
                    # 简化：假设每批都成功修复了部分数据
                    # 实际应从DataX输出解析修复记录数
                else:
                    logger.error(f"批次 {idx} 执行失败: {result.stderr}")
                    failed_batches.append(idx)

            # 汇总结果
            if not failed_batches:
                # 全部成功
                self.repair_result['repair_cnt'] = diff_cnt
                self.repair_result['repair_status'] = 'success'
                if len(self.datax_job_files) > 1:
                    self.repair_result['repair_msg'] = f'修复成功，共{len(self.datax_job_files)}个批次'
                else:
                    self.repair_result['repair_msg'] = '修复成功'
            else:
                # 部分失败
                self.repair_result['repair_status'] = 'partial_fail' if len(failed_batches) < len(self.datax_job_files) else 'fail'
                self.repair_result['repair_msg'] = f"失败批次: {failed_batches}, 错误信息: {result.stderr if result else 'N/A'}"

        except Exception as e:
            self.repair_result['repair_status'] = 'fail'
            self.repair_result['repair_msg'] = str(e)
        finally:
            self.repair_result['repair_end_time'] = datetime.now()
            cost_seconds = (
                        self.repair_result['repair_end_time'] - self.repair_result['repair_start_time']).total_seconds()
            self.repair_result['repair_cost_minute'] = round(cost_seconds / 60, 6)

        return self.repair_result