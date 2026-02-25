#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 16:17
# @Author  : hejun

from abc import ABC, abstractmethod
from typing import Dict, Any
from datetime import datetime


class BaseRepairEngine(ABC):
    """修复引擎基类"""

    def __init__(self, config: Dict[str, Any], compare_result: Dict[str, Any]):
        self.config = config
        self.compare_result = compare_result
        self.repair_result: Dict[str, Any] = {
            'repair_status': 'pending',
            'repair_msg': '',
            'repair_cnt': 0,
            'repair_start_time': None,
            'repair_end_time': None,
            'repair_cost_minute': 0.0,
            'repair_job_file': ''
        }

    @abstractmethod
    def repair(self) -> Dict[str, Any]:
        """执行修复"""
        pass

    def check_repair_conditions(self) -> bool:
        """检查修复条件"""
        from config.settings import TIME_TOLERANCE
        from utils.db_utils import get_table_exists, get_table_writable
        from core.db_adapter.base_adapter import get_db_adapter

        # 条件1：差异记录数 > 0
        if self.compare_result.get('diff_cnt', 0) <= 0:
            self.repair_result['repair_msg'] = "无差异记录，无需修复"
            return False

        # 条件2：目标表存在且可写
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
            if not get_table_exists(tgt_adapter, self.config['tgt_db_name'], self.config['tgt_table_name']):
                self.repair_result['repair_msg'] = "目标表不存在"
                return False

            if not get_table_writable(tgt_adapter, self.config['tgt_db_name'], self.config['tgt_table_name']):
                self.repair_result['repair_msg'] = "目标表不可写"
                return False
        finally:
            tgt_adapter.close()

        # 条件3：源端更新时间 > 目标端更新时间 + 时间容差
        if self.config.get('update_time_str'):
            # 此处需根据实际数据判断更新时间，示例逻辑
            src_update_time = self.compare_result.get('src_update_time', datetime.now())
            tgt_update_time = self.compare_result.get('tgt_update_time', datetime.now())
            time_diff = (src_update_time - tgt_update_time).total_seconds()
            if time_diff <= TIME_TOLERANCE:
                self.repair_result['repair_msg'] = f"源端更新时间未超过目标端+时间容差（{TIME_TOLERANCE}秒）"
                return False

        return True