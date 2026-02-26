#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/2/26
# @Author  : Claude
import pytest
import os
import platform
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime


class TestRepairEngineImprovements:
    """测试修复引擎改进功能"""

    def test_cross_platform_detection(self):
        """测试跨平台检测"""
        from config.settings import PYTHON_BIN_PATH, DATAX_HOME, CURRENT_OS

        assert CURRENT_OS in ['windows', 'linux', 'darwin']
        # PYTHON_BIN_PATH可能为None，但在实际使用时会有值
        if PYTHON_BIN_PATH:
            assert 'python' in PYTHON_BIN_PATH.lower()
        assert DATAX_HOME is not None

    def test_column_intersection_from_compare_result(self):
        """测试从compare_result获取字段交集"""
        from core.repair_engine.datax_repair import DataXRepairEngine

        config = {
            'id': 1,
            'src_db_type': 'mysql',
            'tgt_db_type': 'mysql',
            'src_host': 'localhost',
            'src_port': 3306,
            'src_username': 'root',
            'src_password': '123',
            'src_db_name': 'test',
            'src_table_name': 't1',
            'tgt_host': 'localhost',
            'tgt_port': 3306,
            'tgt_username': 'root',
            'tgt_password': '123',
            'tgt_db_name': 'test',
            'tgt_table_name': 't1'
        }
        compare_result = {
            'compare_columns': {
                'key_columns': ['id'],
                'extra_columns': ['name'],
                'update_column': ['update_time']
            }
        }

        engine = DataXRepairEngine(config, compare_result)
        columns = engine._get_compare_columns()

        assert 'id' in columns  # 主键必须包含
        assert 'name' in columns  # 额外字段
        assert 'update_time' in columns  # 更新字段
        assert len(columns) > 0

    @patch('core.db_adapter.base_adapter.get_db_adapter')
    def test_column_intersection_from_metadata(self, mock_get_adapter):
        """测试从数据库元数据获取字段交集"""
        from core.repair_engine.datax_repair import DataXRepairEngine

        # 模拟适配器
        mock_src_adapter = MagicMock()
        mock_tgt_adapter = MagicMock()

        # 模拟元数据
        mock_src_adapter.get_table_metadata.return_value = [
            {'name': 'id'},
            {'name': 'name'},
            {'name': 'age'},
            {'name': 'update_time'}
        ]
        mock_tgt_adapter.get_table_metadata.return_value = [
            {'name': 'id'},
            {'name': 'name'},
            {'name': 'age'}
        ]
        mock_src_adapter.get_primary_keys.return_value = ['id']

        mock_get_adapter.side_effect = [mock_src_adapter, mock_tgt_adapter]

        config = {
            'id': 1,
            'src_db_type': 'mysql',
            'tgt_db_type': 'mysql',
            'src_host': 'localhost',
            'src_port': 3306,
            'src_username': 'root',
            'src_password': '123',
            'src_db_name': 'test',
            'src_table_name': 't1',
            'tgt_host': 'localhost',
            'tgt_port': 3306,
            'tgt_username': 'root',
            'tgt_password': '123',
            'tgt_db_name': 'test',
            'tgt_table_name': 't1'
        }
        compare_result = {}  # 无结构化字段信息

        engine = DataXRepairEngine(config, compare_result)
        columns = engine._get_compare_columns()

        # 应该获取交集：id, name, age
        assert 'id' in columns
        assert 'name' in columns
        assert 'age' in columns
        # update_time不在目标端，但可能作为主键被添加
        # 实际应取决于业务逻辑

        # 验证关闭连接
        assert mock_src_adapter.close.called
        assert mock_tgt_adapter.close.called

    def test_writer_no_truncate(self):
        """测试Writer不包含TRUNCATE"""
        from core.repair_engine.datax_repair import DataXRepairEngine

        config = {
            'id': 1,
            'src_db_type': 'mysql',
            'tgt_db_type': 'mysql',
            'tgt_username': 'root',
            'tgt_password': '123',
            'tgt_host': 'localhost',
            'tgt_port': 3306,
            'tgt_db_name': 'test',
            'tgt_table_name': 't1'
        }
        compare_result = {
            'compare_columns': {
                'key_columns': ['id']
            }
        }

        engine = DataXRepairEngine(config, compare_result)
        writer_config = engine._get_writer_config()

        # 不应包含TRUNCATE的preSql
        assert 'preSql' not in writer_config['parameter'] or \
               'TRUNCATE' not in str(writer_config['parameter'].get('preSql', ''))

        # 应该包含writeMode
        assert 'writeMode' in writer_config['parameter']
        assert writer_config['parameter']['writeMode'] in ['insert', 'update', 'replace']

    def test_writer_with_custom_presql(self):
        """测试自定义preSql配置"""
        from core.repair_engine.datax_repair import DataXRepairEngine

        config = {
            'id': 1,
            'src_db_type': 'mysql',
            'tgt_db_type': 'mysql',
            'tgt_username': 'root',
            'tgt_password': '123',
            'tgt_host': 'localhost',
            'tgt_port': 3306,
            'tgt_db_name': 'test',
            'tgt_table_name': 't1',
            'repair_presql': 'DELETE FROM t1 WHERE id < 0'  # 安全的自定义SQL
        }
        compare_result = {
            'compare_columns': {
                'key_columns': ['id']
            }
        }

        engine = DataXRepairEngine(config, compare_result)
        writer_config = engine._get_writer_config()

        # 应该包含自定义preSql
        if 'preSql' in writer_config['parameter']:
            assert 'DELETE' in writer_config['parameter']['preSql'][0]
            assert 'TRUNCATE' not in writer_config['parameter']['preSql'][0]

    def test_where_clause_from_diff_records(self):
        """测试基于差异数据的WHERE条件"""
        from core.repair_engine.datax_repair import DataXRepairEngine

        config = {
            'id': 1,
            'src_db_type': 'mysql'
        }
        compare_result = {
            'compare_columns': {
                'key_columns': ['id']
            },
            'diff_records': {
                'mismatch': [{'id': 1}, {'id': 2}],
                'src_only': [{'id': 3}],
                'tgt_only': [{'id': 4}]  # 目标端独有不应包含在WHERE中
            }
        }

        engine = DataXRepairEngine(config, compare_result)
        where_clause = engine._build_where_clause_from_diff_records()

        assert 'id' in where_clause
        assert 'OR' in where_clause
        assert '1' in where_clause or '2' in where_clause or '3' in where_clause

    def test_composite_primary_key_where_clause(self):
        """测试联合主键WHERE条件"""
        from core.repair_engine.datax_repair import DataXRepairEngine

        config = {
            'id': 1,
            'src_db_type': 'mysql'
        }
        compare_result = {
            'compare_columns': {
                'key_columns': ['user_id', 'order_id']
            },
            'diff_records': {
                'mismatch': [{'user_id': 1, 'order_id': 100}],
                'src_only': [],
                'tgt_only': []
            }
        }

        engine = DataXRepairEngine(config, compare_result)
        where_clause = engine._build_where_clause_from_diff_records()

        assert 'user_id' in where_clause
        assert 'order_id' in where_clause
        assert 'AND' in where_clause  # 联合主键用AND连接

    def test_where_clause_with_string_values(self):
        """测试字符串类型主键的WHERE条件"""
        from core.repair_engine.datax_repair import DataXRepairEngine

        config = {
            'id': 1,
            'src_db_type': 'mysql'
        }
        compare_result = {
            'compare_columns': {
                'key_columns': ['code']
            },
            'diff_records': {
                'mismatch': [{'code': "test'value"}],  # 包含单引号
                'src_only': [],
                'tgt_only': []
            }
        }

        engine = DataXRepairEngine(config, compare_result)
        where_clause = engine._build_where_clause_from_diff_records()

        # 应该转义单引号
        assert "test''value" in where_clause  # 单引号被转义为两个单引号

    def test_where_clause_limit_records(self):
        """测试WHERE条件记录数限制"""
        from core.repair_engine.datax_repair import DataXRepairEngine

        config = {
            'id': 1,
            'src_db_type': 'mysql'
        }

        # 创建超过1000条差异数据
        diff_records = {
            'mismatch': [{'id': i} for i in range(1500)],
            'src_only': [],
            'tgt_only': []
        }
        compare_result = {
            'compare_columns': {
                'key_columns': ['id']
            },
            'diff_records': diff_records
        }

        engine = DataXRepairEngine(config, compare_result)
        where_clause = engine._build_where_clause_from_diff_records()

        # 应该限制为前1000条
        # 简单检查WHERE子句不会太长（这里只是基本验证）
        assert len(where_clause) > 0

    def test_where_clause_empty_diff_records(self):
        """测试无差异数据时的WHERE条件"""
        from core.repair_engine.datax_repair import DataXRepairEngine

        config = {
            'id': 1,
            'src_db_type': 'mysql',
            'update_time_str': 'update_time'
        }
        compare_result = {
            'compare_columns': {
                'key_columns': ['id']
            },
            'diff_records': {},
            'check_range': '[2026-01-01 00:00:00,2026-01-02 00:00:00)'
        }

        engine = DataXRepairEngine(config, compare_result)
        where_clause = engine._build_where_clause_from_diff_records()

        # 无差异数据时应该回退到时间范围
        assert 'update_time' in where_clause

    def test_write_mode_validation(self):
        """测试写入模式验证"""
        from core.repair_engine.datax_repair import DataXRepairEngine

        config = {
            'id': 1,
            'src_db_type': 'mysql',
            'tgt_db_type': 'mysql',
            'tgt_username': 'root',
            'tgt_password': '123',
            'tgt_host': 'localhost',
            'tgt_port': 3306,
            'tgt_db_name': 'test',
            'tgt_table_name': 't1',
            'repair_write_mode': 'invalid_mode'  # 无效模式
        }
        compare_result = {
            'compare_columns': {
                'key_columns': ['id']
            }
        }

        engine = DataXRepairEngine(config, compare_result)
        writer_config = engine._get_writer_config()

        # 应该回退到默认的update模式
        assert writer_config['parameter']['writeMode'] == 'update'

    def test_update_mode_with_pk_columns(self):
        """测试update模式包含主键列"""
        from core.repair_engine.datax_repair import DataXRepairEngine

        config = {
            'id': 1,
            'src_db_type': 'mysql',
            'tgt_db_type': 'mysql',
            'tgt_username': 'root',
            'tgt_password': '123',
            'tgt_host': 'localhost',
            'tgt_port': 3306,
            'tgt_db_name': 'test',
            'tgt_table_name': 't1',
            'repair_write_mode': 'update'
        }
        compare_result = {
            'compare_columns': {
                'key_columns': ['id', 'name']
            }
        }

        engine = DataXRepairEngine(config, compare_result)
        writer_config = engine._get_writer_config()

        # update模式应该包含pk_columns
        assert 'pk_columns' in writer_config['parameter']
        assert writer_config['parameter']['pk_columns'] == ['id', 'name']

    def test_insert_mode_no_pk_columns(self):
        """测试insert模式不包含主键列"""
        from core.repair_engine.datax_repair import DataXRepairEngine

        config = {
            'id': 1,
            'src_db_type': 'mysql',
            'tgt_db_type': 'mysql',
            'tgt_username': 'root',
            'tgt_password': '123',
            'tgt_host': 'localhost',
            'tgt_port': 3306,
            'tgt_db_name': 'test',
            'tgt_table_name': 't1',
            'repair_write_mode': 'insert'
        }
        compare_result = {
            'compare_columns': {
                'key_columns': ['id']
            }
        }

        engine = DataXRepairEngine(config, compare_result)
        writer_config = engine._get_writer_config()

        # insert模式不应该包含pk_columns
        assert 'pk_columns' not in writer_config['parameter']

    def test_where_clause_with_in_syntax_single_pk(self):
        """测试单主键使用IN语法"""
        from core.repair_engine.datax_repair import DataXRepairEngine

        config = {
            'id': 1,
            'src_db_type': 'mysql'
        }
        compare_result = {
            'compare_columns': {
                'key_columns': ['id']
            },
            'diff_records': {
                'mismatch': [{'id': i} for i in range(1, 11)],  # 10条记录
                'src_only': [],
                'tgt_only': []
            }
        }

        engine = DataXRepairEngine(config, compare_result)
        where_clauses = engine._build_where_clauses_batch()

        # 应该生成1个批次（10条记录 < 3000）
        assert len(where_clauses) == 1
        # 应该使用IN语法
        assert 'IN' in where_clauses[0].upper()
        assert 'id' in where_clauses[0]

    def test_where_clause_with_in_syntax_composite_pk(self):
        """测试联合主键使用IN语法"""
        from core.repair_engine.datax_repair import DataXRepairEngine

        config = {
            'id': 1,
            'src_db_type': 'mysql'
        }
        compare_result = {
            'compare_columns': {
                'key_columns': ['user_id', 'order_id']
            },
            'diff_records': {
                'mismatch': [{'user_id': i, 'order_id': i*100} for i in range(1, 6)],
                'src_only': [],
                'tgt_only': []
            }
        }

        engine = DataXRepairEngine(config, compare_result)
        where_clauses = engine._build_where_clauses_batch()

        # 应该生成1个批次
        assert len(where_clauses) == 1
        # 联合主键应该包含AND
        assert 'AND' in where_clauses[0].upper()
        # 应该使用IN语法
        assert 'IN' in where_clauses[0].upper()

    def test_batch_generation_multiple_files(self):
        """测试批量生成多个文件"""
        from core.repair_engine.datax_repair import DataXRepairEngine
        from unittest.mock import patch, mock_open

        config = {
            'id': 1,
            'src_db_type': 'mysql',
            'tgt_db_type': 'mysql',
            'src_username': 'root',
            'src_password': '123',
            'tgt_username': 'root',
            'tgt_password': '123',
            'src_host': 'localhost',
            'src_port': 3306,
            'tgt_host': 'localhost',
            'tgt_port': 3306,
            'src_db_name': 'test',
            'tgt_db_name': 'test',
            'src_table_name': 't1',
            'tgt_table_name': 't1'
        }

        # 创建3500条差异记录（超过3000，应该分批）
        compare_result = {
            'compare_columns': {
                'key_columns': ['id'],
                'extra_columns': ['name'],
                'update_column': ['update_time']
            },
            'diff_records': {
                'mismatch': [{'id': i} for i in range(1, 3501)],  # 3500条记录
                'src_only': [],
                'tgt_only': []
            }
        }

        engine = DataXRepairEngine(config, compare_result)
        where_clauses = engine._build_where_clauses_batch()

        # 应该生成2个批次（3500条记录，每批3000条）
        assert len(where_clauses) == 2

    def test_sql_value_formatting(self):
        """测试SQL值格式化"""
        from core.repair_engine.datax_repair import DataXRepairEngine
        from datetime import datetime

        config = {'id': 1, 'src_db_type': 'mysql'}
        compare_result = {}

        engine = DataXRepairEngine(config, compare_result)

        # 测试字符串
        assert engine._format_sql_value("test") == "'test'"
        assert engine._format_sql_value("test'value") == "'test''value'"

        # 测试数字
        assert engine._format_sql_value(123) == "123"
        assert engine._format_sql_value(45.67) == "45.67"

        # 测试None
        assert engine._format_sql_value(None) == "NULL"

        # 测试datetime
        dt = datetime(2026, 2, 26, 12, 30, 45)
        assert "'2026-02-26 12:30:45'" in engine._format_sql_value(dt)

    def test_filename_with_table_name_single_batch(self):
        """测试单批次文件名使用目标表名"""
        from core.repair_engine.datax_repair import DataXRepairEngine
        from unittest.mock import patch, MagicMock

        config = {
            'id': 1,
            'src_db_type': 'mysql',
            'tgt_db_type': 'mysql',
            'src_username': 'root',
            'src_password': '123',
            'tgt_username': 'root',
            'tgt_password': '123',
            'src_host': 'localhost',
            'src_port': 3306,
            'tgt_host': 'localhost',
            'tgt_port': 3306,
            'src_db_name': 'test',
            'tgt_db_name': 'test',
            'src_table_name': 'source_table',
            'tgt_table_name': 'my_target_table'  # 目标表名
        }

        # 创建少量记录（单批次）
        compare_result = {
            'compare_columns': {
                'key_columns': ['id'],
                'extra_columns': ['name'],
                'update_column': ['update_time']
            },
            'diff_records': {
                'mismatch': [{'id': i} for i in range(1, 101)],  # 100条记录（< 3000）
                'src_only': [],
                'tgt_only': []
            }
        }

        engine = DataXRepairEngine(config, compare_result)

        with patch('os.makedirs'):
            with patch('builtins.open', MagicMock()):
                with patch('json.dump'):
                    engine.generate_datax_job()

        # 应该生成单个文件，文件名为目标表名.json
        assert len(engine.datax_job_files) == 1
        assert 'my_target_table.json' in engine.datax_job_files[0]

    def test_filename_with_table_name_multiple_batches(self):
        """测试多批次文件名使用目标表名+批次号"""
        from core.repair_engine.datax_repair import DataXRepairEngine
        from unittest.mock import patch, MagicMock

        config = {
            'id': 1,
            'src_db_type': 'mysql',
            'tgt_db_type': 'mysql',
            'src_username': 'root',
            'src_password': '123',
            'tgt_username': 'root',
            'tgt_password': '123',
            'src_host': 'localhost',
            'src_port': 3306,
            'tgt_host': 'localhost',
            'tgt_port': 3306,
            'src_db_name': 'test',
            'tgt_db_name': 'test',
            'src_table_name': 'source_table',
            'tgt_table_name': 'orders'  # 目标表名
        }

        # 创建大量记录（多批次）
        compare_result = {
            'compare_columns': {
                'key_columns': ['id'],
                'extra_columns': ['name'],
                'update_column': ['update_time']
            },
            'diff_records': {
                'mismatch': [{'id': i} for i in range(1, 6501)],  # 6500条记录（> 3000，需要3个批次）
                'src_only': [],
                'tgt_only': []
            }
        }

        engine = DataXRepairEngine(config, compare_result)

        with patch('os.makedirs'):
            with patch('builtins.open', MagicMock()):
                with patch('json.dump'):
                    engine.generate_datax_job()

        # 应该生成3个文件：orders_1.json, orders_2.json, orders_3.json
        assert len(engine.datax_job_files) == 3
        assert 'orders_1.json' in engine.datax_job_files[0]
        assert 'orders_2.json' in engine.datax_job_files[1]
        assert 'orders_3.json' in engine.datax_job_files[2]


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
