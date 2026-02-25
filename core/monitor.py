#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 16:25
# @Author  : hejun
import time
from typing import Dict, Any
from prometheus_client import Counter, Gauge, Histogram, Summary
from apscheduler.schedulers.background import BackgroundScheduler
import logging

logger = logging.getLogger(__name__)

# 定义监控指标
COMPARE_TASK_COUNTER = Counter('compare_tasks_total', 'Total compare tasks', ['status'])
REPAIR_TASK_COUNTER = Counter('repair_tasks_total', 'Total repair tasks', ['status'])
TASK_DURATION_SUMMARY = Summary('task_duration_seconds', 'Task duration summary')
TABLE_RECORD_GAUGE = Gauge('table_records', 'Table record count', ['table', 'database'])
COMPARE_DIFFERENCE_GAUGE = Gauge('compare_differences', 'Compare differences count', ['table'])

class Monitor:
    """监控指标收集类"""

    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.start_monitoring()

    def start_monitoring(self):
        """启动监控"""
        self.scheduler.start()
        logger.info("监控系统已启动")

    def stop_monitoring(self):
        """停止监控"""
        self.scheduler.shutdown()
        logger.info("监控系统已停止")

    def record_compare_task(self, status: str):
        """记录比对任务"""
        COMPARE_TASK_COUNTER.labels(status=status).inc()

    def record_repair_task(self, status: str):
        """记录修复任务"""
        REPAIR_TASK_COUNTER.labels(status=status).inc()

    def record_task_duration(self, duration: float):
        """记录任务耗时"""
        TASK_DURATION_SUMMARY.observe(duration)

    def record_table_records(self, table_name: str, db_name: str, count: int):
        """记录表记录数"""
        TABLE_RECORD_GAUGE.labels(table=table_name, database=db_name).set(count)

    def record_compare_difference(self, table_name: str, diff_count: int):
        """记录比对差异"""
        COMPARE_DIFFERENCE_GAUGE.labels(table=table_name).set(diff_count)

    def get_metrics(self) -> Dict[str, Any]:
        """获取监控指标"""
        from prometheus_client import generate_latest, REGISTRY
        
        metrics_text = generate_latest(REGISTRY).decode('utf-8')
        return {'metrics': metrics_text}


# 全局监控实例
monitor = Monitor()


def get_monitor():
    """获取监控实例"""
    return monitor