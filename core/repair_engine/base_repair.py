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