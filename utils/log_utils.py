#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/2/27
# @Author  : hejun
"""日志配置工具类"""
import logging
import sys
import os


def setup_logging(log_dir='logs', log_file='data-consistency-platform.log', log_level='INFO'):
    """
    配置应用程序日志

    Args:
        log_dir: 日志目录
        log_file: 日志文件名
        log_level: 日志级别

    配置说明：
    - 文件日志：详细格式，包含文件名和行号
    - 控制台日志：简洁格式，输出到stdout避免PyCharm红色显示
    - 第三方库日志：只输出WARNING及以上级别
    """
    # 确保日志目录存在
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, log_file)

    # 创建格式化器
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # 清除现有处理器（避免重复添加）
    root_logger.handlers.clear()

    # 文件处理器 - 详细日志
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)

    # 控制台处理器 - 使用stdout避免PyCharm红色显示
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler.setFormatter(console_formatter)

    # 添加处理器
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # 抑制第三方库的详细日志
    logging.getLogger('datacompy').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

    return root_logger


def get_logger(name):
    """获取指定名称的logger"""
    return logging.getLogger(name)
