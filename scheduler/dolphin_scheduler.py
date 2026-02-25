#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 16:17
# @Author  : hejun
import logging
import requests
import json
from typing import Dict, Any
from config.settings import DOLPHINSCHEDULER_CONFIG

logger = logging.getLogger(__name__)


class DolphinSchedulerClient:
    """DolphinScheduler客户端"""

    def __init__(self):
        self.base_url = DOLPHINSCHEDULER_CONFIG['base_url']
        self.username = DOLPHINSCHEDULER_CONFIG['username']
        self.password = DOLPHINSCHEDULER_CONFIG['password']
        self.session_id = self._login()

    def _login(self) -> str:
        """登录获取sessionId"""
        url = f"{self.base_url}/dolphinscheduler/login"
        data = {
            "userName": self.username,
            "userPassword": self.password
        }
        response = requests.post(url, json=data, timeout=30)
        response.raise_for_status()
        result = response.json()
        if result.get('code') != 0:
            raise ValueError(f"DolphinScheduler登录失败：{result.get('msg')}")
        return result.get('data').get('sessionId')

    def submit_compare_task(self, task_config: Dict[str, Any]) -> int:
        """提交比对任务到DolphinScheduler"""
        url = f"{self.base_url}/dolphinscheduler/projects/{DOLPHINSCHEDULER_CONFIG['project_name']}/process-instances/start"

        # 构建任务参数
        process_params = {
            "table_id": task_config.get('table_id'),
            "incremental": task_config.get('incremental', False),
            "incremental_days": task_config.get('incremental_days', 1),
            "enable_repair": task_config.get('enable_repair', False)
        }

        data = {
            "processDefinitionCode": DOLPHINSCHEDULER_CONFIG['process_code'],
            "failureStrategy": "END",
            "warningType": "NONE",
            "processInstanceParams": {
                "localParams": [
                    {"prop": k, "value": v, "type": "VARCHAR"} for k, v in process_params.items()
                ]
            }
        }

        headers = {"sessionId": self.session_id}
        response = requests.post(url, json=data, headers=headers, timeout=30)
        response.raise_for_status()

        result = response.json()
        if result.get('code') != 0:
            raise ValueError(f"提交任务失败：{result.get('msg')}")

        task_id = result.get('data').get('processInstanceId')
        logger.info(f"任务提交成功，实例ID：{task_id}")
        return task_id

    def get_task_status(self, task_id: int) -> str:
        """获取任务状态"""
        url = f"{self.base_url}/dolphinscheduler/process-instances/{task_id}"
        headers = {"sessionId": self.session_id}
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        result = response.json()
        if result.get('code') != 0:
            raise ValueError(f"获取任务状态失败：{result.get('msg')}")

        return result.get('data').get('state')