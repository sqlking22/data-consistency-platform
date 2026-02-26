#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pytest配置文件和测试夹具
"""
import pytest
import os
import sys
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

# 添加项目根目录到sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock未安装的可选依赖模块,避免测试环境依赖
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['cx_Oracle'] = MagicMock()
sys.modules['psycopg2'] = MagicMock()
sys.modules['pyodbc'] = MagicMock()


@pytest.fixture
def sample_config():
    """示例任务配置"""
    return {
        'id': 1,
        'table_id': 1,
        'src_db_id': 100,
        'src_db_type': 'mysql',
        'src_host': 'localhost',
        'src_port': 3306,
        'src_username': 'root',
        'src_password': '123456',
        'src_db_name': 'test_db',
        'src_table_name': 'source_table',
        'tgt_db_type': 'mysql',
        'tgt_host': 'localhost',
        'tgt_port': 3306,
        'tgt_username': 'root',
        'tgt_password': '123456',
        'tgt_db_name': 'test_db',
        'tgt_table_name': 'target_table',
        'update_time_str': 'update_time',
        'sensitive_str': '',
        'incremental': False,
        'incremental_days': 1,
        'enable_repair': True,
        'time_tolerance': 300,
        'retry_times': 3,
        'retry_delay': 5,
        'batch_size': 1000,
        'max_records_threshold': 300001,
        'max_diff_records_threshold': 200000,
        'max_repair_records_threshold': 3000
    }


@pytest.fixture
def sample_incremental_config(sample_config):
    """增量比对配置"""
    config = sample_config.copy()
    config['incremental'] = True
    config['incremental_days'] = 3
    return config


@pytest.fixture
def sample_db_config():
    """示例数据库配置"""
    return {
        'db_type': 'mysql',
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '123456',
        'database': 'test_db'
    }


@pytest.fixture
def sample_table_metadata():
    """示例表元数据"""
    return [
        {'name': 'id', 'type': 'int', 'nullable': False, 'primary_key': True},
        {'name': 'name', 'type': 'varchar(100)', 'nullable': True},
        {'name': 'age', 'type': 'int', 'nullable': True},
        {'name': 'update_time', 'type': 'datetime', 'nullable': True},
        {'name': 'salary', 'type': 'decimal(10,2)', 'nullable': True}
    ]


@pytest.fixture
def sample_primary_keys():
    """示例主键列表"""
    return ['id']


@pytest.fixture
def sample_source_data():
    """示例源端数据"""
    return [
        {'id': 1, 'name': 'Alice', 'age': 30, 'update_time': datetime(2026, 2, 25, 10, 0, 0), 'salary': 5000.00},
        {'id': 2, 'name': 'Bob', 'age': 25, 'update_time': datetime(2026, 2, 25, 11, 0, 0), 'salary': 6000.00},
        {'id': 3, 'name': 'Charlie', 'age': 35, 'update_time': datetime(2026, 2, 25, 12, 0, 0), 'salary': 7000.00},
    ]


@pytest.fixture
def sample_target_data():
    """示例目标端数据"""
    return [
        {'id': 1, 'name': 'Alice', 'age': 30, 'update_time': datetime(2026, 2, 25, 10, 0, 0), 'salary': 5000.00},
        {'id': 2, 'name': 'Bob Modified', 'age': 26, 'update_time': datetime(2026, 2, 25, 11, 30, 0), 'salary': 6500.00},
        # id=3 缺失
    ]


@pytest.fixture
def sample_compare_result():
    """示例比对结果"""
    return {
        'src_cnt': 3,
        'tgt_cnt': 2,
        'diff_cnt': 2,
        'compare_status': 'success',
        'compare_msg': '比对成功',
        'matching_rate': 0.33,
        'check_range': '[2026-02-22 00:00:00,2026-02-25 00:00:00)',
        'check_column': "key_columns：['id'],update_column: ['update_time'],extra_columns: ['age', 'salary']",
        'compare_columns': {
            'key_columns': ['id'],
            'update_column': ['update_time'],
            'extra_columns': ['age', 'salary']
        },
        'compare_report': '发现2条差异记录',
        'html_report': '<html>...</html>',
        'compare_start_time': datetime(2026, 2, 25, 10, 0, 0),
        'compare_end_time': datetime(2026, 2, 25, 10, 5, 0),
        'compare_cost_minute': 5.0
    }


@pytest.fixture
def sample_repair_result():
    """示例修复结果"""
    return {
        'repair_status': 'success',
        'repair_cnt': 2,
        'repair_msg': '修复成功',
        'repair_start_time': datetime(2026, 2, 25, 10, 5, 0),
        'repair_end_time': datetime(2026, 2, 25, 10, 10, 0),
        'repair_cost_minute': 5.0,
        'repair_job_file': '/data/datax-3.0/job/20260225/repair_1_20260225100500.json'
    }


@pytest.fixture
def mock_db_adapter():
    """模拟数据库适配器"""
    adapter = Mock()
    adapter.connect = Mock()
    adapter.close = Mock()
    adapter.query = Mock(return_value=[])
    adapter.execute = Mock(return_value=1)
    adapter.get_table_metadata = Mock(return_value=[])
    adapter.get_primary_keys = Mock(return_value=['id'])
    adapter.get_table_count = Mock(return_value=100)
    adapter.query_data = Mock(return_value=[])
    adapter.get_extra_columns = Mock(return_value=['age', 'salary'])
    return adapter


@pytest.fixture
def temp_config_file(tmp_path):
    """临时配置文件"""
    config_data = {
        "id": 1,
        "src_db_type": "mysql",
        "src_host": "localhost",
        "src_port": 3306,
        "src_username": "root",
        "src_password": "123456",
        "src_db_name": "test_db",
        "src_table_name": "source_table",
        "tgt_db_type": "mysql",
        "tgt_host": "localhost",
        "tgt_port": 3306,
        "tgt_username": "root",
        "tgt_password": "123456",
        "tgt_db_name": "test_db",
        "tgt_table_name": "target_table",
        "update_time_str": "update_time",
        "sensitive_str": ""
    }

    config_file = tmp_path / "test_config.json"
    import json
    with open(config_file, 'w', encoding='utf-8') as f:
        json.dump(config_data, f)

    return str(config_file)


@pytest.fixture
def mock_requests_post():
    """模拟requests.post"""
    with patch('requests.post') as mock_post:
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response
        yield mock_post
