#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 12:42
# @Author  : hejun
import os
import platform
import shutil
from datetime import datetime

# 检测操作系统
CURRENT_OS = platform.system().lower()  # 'windows', 'linux', 'darwin'

# 日志配置
LOG_LEVEL = 'INFO'
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../logs')
os.makedirs(LOG_DIR, exist_ok=True)

# 企业微信机器人配置
WX_WORK_ROBOT = "fbf38202-fad2-4fae-a30a-cf83c4f644ae"
WX_AT_LIST = ['hejun']
# WX_ALERT_THRESHOLD = 0.05  # 差异率超过5%触发告警
WX_ALERT_THRESHOLD = 0.00 # 差异率超过5%触发告警

# DataX相关配置 - 跨平台支持
# Python可执行文件路径 - 跨平台支持
if CURRENT_OS == 'windows':
    # Windows: 优先使用PATH中的python，否则使用常见路径
    PYTHON_BIN_PATH = shutil.which('python') or \
                      shutil.which('python3') or \
                      r'C:\Python311\python.exe'
else:
    # Linux/Unix: 使用常见的python路径
    PYTHON_BIN_PATH = shutil.which('python3') or \
                      shutil.which('python') or \
                      '/usr/bin/python311'

# DataX主目录 - 跨平台支持
if CURRENT_OS == 'windows':
    DATAX_HOME = r'D:\software_pkg\datax'  # Windows默认路径
else:
    DATAX_HOME = '/data/datax-3.0'  # Linux默认路径

# 允许通过环境变量覆盖
DATAX_HOME = os.getenv('DATAX_HOME', DATAX_HOME)
PYTHON_BIN_PATH = os.getenv('PYTHON_BIN_PATH', PYTHON_BIN_PATH)

# 验证路径存在性（仅警告，不阻止启动）
if not os.path.exists(DATAX_HOME):
    print(f"警告: DATAX_HOME路径不存在: {DATAX_HOME}")
if PYTHON_BIN_PATH and not os.path.exists(PYTHON_BIN_PATH):
    print(f"警告: PYTHON_BIN_PATH不存在: {PYTHON_BIN_PATH}")

DATAX_BIN = os.path.join(DATAX_HOME, "bin", "datax.py")
DATAX_JOB_DIR = os.path.join(DATAX_HOME, "job", datetime.now().strftime("%Y%m%d"))
DATAX_LOG_LEVEL = 'INFO'
os.makedirs(DATAX_JOB_DIR, exist_ok=True)

# 任务数据库连接配置
TASK_DB_CONFIG = {
    "db_type": "mysql",
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "database": "test",
    "charset": 'utf8'
}

# 任务配置表和日志表
TASK_CONFIG_TABLE = "task_config_info"
TASK_LOG_TABLE = "task_result_log"

# 批量处理配置
BATCH_SIZE = 1000  # 批量插入大小
MAX_RECORDS_THRESHOLD = 300001  # 表记录数阈值
MAX_DIFF_RECORDS_THRESHOLD = 200000  # 差异记录数阈值
MAX_REPAIR_RECORDS_THRESHOLD = 3000  # 可修复记录数阈值
CHUNK_SIZE_FOR_DATA_SYNC = 10000  # 数据同步分块大小
RECORDS_PER_THREAD = 50000  # 每线程处理记录数
MAX_THREAD_COUNT = 3  # 最大线程数

# 比对策略配置
ENGINE_STRATEGY = "auto"  # pandas/spark_local/spark_cluster/auto
# ENABLE_REPAIR = False
# IS_INCREMENTAL = False

ENABLE_REPAIR = True
IS_INCREMENTAL = True
INCREMENTAL_DAYS = 1
TIME_TOLERANCE = 0  # 时间容差（秒）
ENABLE_TIME_FILTER = False  # 是否启用时间字段过滤（比对源端和目标端时间字段）False/True
RETRY_TIMES = 3  # 重试次数
RETRY_DELAY = 5  # 重试延迟（秒）

# 数据库密码是否需要解密
DECODE_PASSWORD_FLAG = False
# 是否需要校验额外的字段
EXTRA_COLUMN_FLAG = True

# 支持的字段类型配置
SUPPORT_COLUMN_TYPE = {
    'mysql': {
        # Dates
        "datetime", "timestamp", "date",
        # Numbers
        "double", "float", "decimal", "int", "bigint", "mediumint", "smallint", "tinyint",
        # Boolean
        "boolean"
    },
    'sqlserver': {
        # Timestamps
        "datetime", "date", "time",
        # Numbers
        "float", "decimal", "int", "bigint", "tinyint", "smallint",
        # Bool
        "bit"
    },
    'oracle': {
        # Dates
        "date", "timestamp",
        # Numbers
        "number", "float", "double", "integer", "smallint"
    },
    'postgresql': {
        # Dates
        "timestamp", "date", "time",
        # Numbers
        "numeric", "float8", "int4", "int8", "int2", "decimal",
        # Boolean
        "boolean"
    }
}

# 加解密配置
ENCRYPT_KEY = "your16bytes-key!"  # AES密钥（16/24/32字节）

# DolphinScheduler配置
DOLPHINSCHEDULER_CONFIG = {
    "base_url": "http://dolphinscheduler-master:12345",
    "username": "admin",
    "password": "dolphinscheduler123",
    "project_name": "data-consistency",
    "process_code": 123456  # 流程定义编码
}

# Spark配置
SPARK_CONFIG = {
    "spark_local": {
        "driver_memory": "8g",
        "executor_memory": "4g",
        "cores": "4"
    },
    "spark_cluster": {
        "master": "yarn",
        "deploy_mode": "cluster"
    }
}

# 修复引擎配置
REPAIR_WRITE_MODE = 'update'  # Options: 'insert', 'update', 'replace'
REPAIR_BATCH_SIZE = 500  # 批量修复时每批记录数
REPAIR_MAX_WHERE_IN_RECORDS = 3000  # WHERE子句中最大记录数（IN语法）
