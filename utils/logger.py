#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 16:20
# @Author  : hejun
import logging
import os
from datetime import datetime
from concurrent_log_handler import ConcurrentRotatingFileHandler
from config.settings import LOG_DIR, LOG_LEVEL


class LogManager:
    """日志管理类"""

    def __init__(self, config=None):
        self.config = config or {}
        self.logger = self._setup_logger()

    def _setup_logger(self):
        """设置日志记录器"""
        # 创建logger
        logger = logging.getLogger(f"table_{self.config.get('table_id', 'unknown')}")
        logger.setLevel(getattr(logging, LOG_LEVEL.upper()))

        # 避免重复添加处理器
        if logger.handlers:
            return logger

        # 设置日志格式
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # 文件处理器（带滚动）
        log_file = os.path.join(LOG_DIR, f"data_consistency_{datetime.now().strftime('%Y%m%d')}.log")
        file_handler = ConcurrentRotatingFileHandler(
            log_file,
            maxBytes=50 * 1024 * 1024,  # 50MB
            backupCount=10
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        return logger

    def get_logger(self):
        """获取logger实例"""
        return self.logger


# 创建一个全局默认logger实例
_default_log_manager = LogManager()
logger = _default_log_manager.get_logger()