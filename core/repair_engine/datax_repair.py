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

    def generate_datax_job(self) -> str:
        """生成DataX作业配置文件"""
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
                        "reader": self._get_reader_config(),
                        "writer": self._get_writer_config()
                    }
                ]
            }
        }
        # 生成作业文件
        job_file_name = f"repair_{self.config['id']}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        self.datax_job_file = os.path.join(DATAX_JOB_DIR, job_file_name)
        print(self.datax_job_file)

        with open(self.datax_job_file, 'w', encoding='utf-8') as f:
            json.dump(job_config, f, ensure_ascii=False, indent=4)

        return self.datax_job_file

    def _get_reader_config(self) -> Dict[str, Any]:
        """生成Reader配置"""

        db_type = self.config['src_db_type'].lower()
        reader_type = {
            'mysql': 'mysqlreader',
            'oracle': 'oraclereader',
            'sqlserver': 'sqlserverreader',
            'postgresql': 'postgresqlreader'
        }.get(db_type, 'mysqlreader')

        # 构建WHERE子句（只同步差异数据）
        where_clause = self.compare_result.get('check_range', '')
        if where_clause:
            where_clause = where_clause.replace('[', '').replace(')', '').replace(']', '')
            start_time, end_time = where_clause.split(',')
            update_col = self.config.get('update_time_str', '')
            if update_col:
                where_clause = f"{update_col} >= '{start_time}' AND {update_col} < '{end_time}'"
        reader_config = {
            "name": reader_type,
            "parameter": {
                "username": self.config['src_username'],
                "password": self.config['src_password'],
                "column": self._get_compare_columns(),
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
        return reader_config

    def _get_writer_config(self) -> Dict[str, Any]:
        """生成Writer配置"""
        db_type = self.config['tgt_db_type'].lower()
        writer_type = {
            'mysql': 'mysqlwriter',
            'oracle': 'oraclewriter',
            'sqlserver': 'sqlserverwriter',
            'postgresql': 'postgresqlwriter'
        }.get(db_type, 'mysqlwriter')

        writer_config = {
            "name": writer_type,
            "parameter": {
                "username": self.config['tgt_username'],
                "password": self.config['tgt_password'],
                "column": self._get_compare_columns(),
                "preSql": [f"TRUNCATE TABLE {self.config['tgt_table_name']}"],  # 根据实际需求调整
                "connection": [
                    {
                        "table": [self.config['tgt_table_name']],
                        "jdbcUrl": self._get_jdbc_url('tgt')
                    }
                ],
                "writeMode": "update"  # 按需选择：insert/update/upsert
            }
        }
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

    def _get_compare_columns(self) -> List[str]:
        """获取需要同步的字段"""
        columns = self.compare_result.get('check_column', '')

        # 解析字段列表
        all_cols = []

        # 使用正则提取字段列表
        key_match = re.search(r'key_columns：\[(.*?)\]', columns)
        if key_match:
            key_str = key_match.group(1)
            key_cols = [col.strip().strip("'\"") for col in key_str.split(',') if col.strip()]
            all_cols.extend(key_cols)

        extra_match = re.search(r'extra_columns：\[(.*?)\]', columns)
        if extra_match:
            extra_str = extra_match.group(1)
            extra_cols = [col.strip().strip("'\"") for col in extra_str.split(',') if col.strip()]
            all_cols.extend(extra_cols)

        # 如果没有解析到,返回常见字段
        if not all_cols:
            # 返回一个默认字段列表
            all_cols = ['id', 'name', 'age', 'update_time', 'salary']

        return all_cols

    def repair(self) -> Dict[str, Any]:
        """执行修复"""
        from config.settings import MAX_REPAIR_RECORDS_THRESHOLD, TIME_TOLERANCE
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
            # 生成DataX作业
            job_file = self.generate_datax_job()

            self.repair_result['repair_job_file'] = job_file

            # 执行DataX作业
            cmd = [PYTHON_BIN_PATH, DATAX_BIN, job_file]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600
            )

            if result.returncode == 0:
                # 解析修复记录数（简化实现）
                self.repair_result['repair_cnt'] = diff_cnt
                self.repair_result['repair_status'] = 'success'
                self.repair_result['repair_msg'] = '修复成功'
            else:
                self.repair_result['repair_status'] = 'fail'
                self.repair_result['repair_msg'] = f"DataX执行失败：{result.stderr}"

        except Exception as e:
            self.repair_result['repair_status'] = 'fail'
            self.repair_result['repair_msg'] = str(e)
        finally:
            self.repair_result['repair_end_time'] = datetime.now()
            cost_seconds = (
                        self.repair_result['repair_end_time'] - self.repair_result['repair_start_time']).total_seconds()
            self.repair_result['repair_cost_minute'] = round(cost_seconds / 60, 6)

        return self.repair_result