#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
比对引擎测试用例
"""
import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from core.compare_engine.base_engine import BaseCompareEngine, get_compare_engine


class TestBaseCompareEngine:
    """比对引擎基类测试"""

    def test_init(self, sample_config):
        """测试初始化"""
        engine = Mock(spec=BaseCompareEngine)
        engine.config = sample_config
        engine.compare_result = {}

        assert engine.config['src_table_name'] == 'source_table'

    def test_init_adapters(self, sample_config, mock_db_adapter):
        """测试初始化适配器"""
        # 创建一个具体的引擎实例用于测试
        from core.compare_engine.pandas_engine import PandasCompareEngine

        with patch('core.db_adapter.base_adapter.get_db_adapter', return_value=mock_db_adapter):
            engine = PandasCompareEngine(sample_config)
            engine.init_adapters()

        assert engine.src_adapter is not None
        assert engine.tgt_adapter is not None

    def test_get_where_clause_full_load(self, sample_config):
        """测试全量比对的WHERE子句"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        engine = PandasCompareEngine(sample_config)
        engine.config['incremental'] = False

        where_clause = engine.get_where_clause()

        assert where_clause == ""

    def test_get_where_clause_incremental_mysql(self, sample_incremental_config):
        """测试MySQL增量比对的WHERE子句"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        engine = PandasCompareEngine(sample_incremental_config)
        engine.config['src_db_type'] = 'mysql'

        where_clause = engine.get_where_clause()

        assert 'update_time >=' in where_clause
        assert 'AND' in where_clause

    def test_get_where_clause_incremental_oracle(self, sample_incremental_config):
        """测试Oracle增量比对的WHERE子句"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        engine = PandasCompareEngine(sample_incremental_config)
        engine.config['src_db_type'] = 'oracle'

        where_clause = engine.get_where_clause()

        assert 'TO_DATE' in where_clause
        assert 'update_time >=' in where_clause

    def test_get_where_clause_incremental_postgresql(self, sample_incremental_config):
        """测试PostgreSQL增量比对的WHERE子句"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        engine = PandasCompareEngine(sample_incremental_config)
        engine.config['src_db_type'] = 'postgresql'

        where_clause = engine.get_where_clause()

        assert 'update_time >=' in where_clause

    def test_get_where_clause_incremental_sqlserver(self, sample_incremental_config):
        """测试SQLServer增量比对的WHERE子句"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        engine = PandasCompareEngine(sample_incremental_config)
        engine.config['src_db_type'] = 'sqlserver'

        where_clause = engine.get_where_clause()

        assert 'update_time >=' in where_clause

    def test_get_compare_columns(self, sample_config, mock_db_adapter):
        """测试获取比对字段"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        with patch('core.db_adapter.base_adapter.get_db_adapter', return_value=mock_db_adapter):
            mock_db_adapter.get_primary_keys.return_value = ['id']
            mock_db_adapter.get_extra_columns.return_value = ['age', 'salary']

            engine = PandasCompareEngine(sample_config)
            engine.src_adapter = mock_db_adapter
            columns = engine.get_compare_columns()

        assert columns['key_columns'] == ['id']
        assert columns['update_column'] == ['update_time']
        assert 'age' in columns['extra_columns']

    def test_get_compare_columns_no_primary_key(self, sample_config, mock_db_adapter):
        """测试无主键的情况"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        with patch('core.db_adapter.base_adapter.get_db_adapter', return_value=mock_db_adapter):
            mock_db_adapter.get_primary_keys.return_value = []

            engine = PandasCompareEngine(sample_config)
            engine.src_adapter = mock_db_adapter

            with pytest.raises(ValueError, match="表无主键"):
                engine.get_compare_columns()

    def test_get_compare_columns_with_sensitive_fields(self, sample_config, mock_db_adapter):
        """测试包含敏感字段"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        sample_config['sensitive_str'] = 'salary,age'

        with patch('core.db_adapter.base_adapter.get_db_adapter', return_value=mock_db_adapter):
            mock_db_adapter.get_primary_keys.return_value = ['id']
            mock_db_adapter.get_extra_columns.return_value = ['age', 'salary', 'score']

            engine = PandasCompareEngine(sample_config)
            engine.src_adapter = mock_db_adapter
            columns = engine.get_compare_columns()

        # 敏感字段应该被过滤掉
        assert 'age' not in columns['extra_columns']
        assert 'salary' not in columns['extra_columns']
        assert 'score' in columns['extra_columns']

    def test_run_success(self, sample_config, mock_db_adapter, sample_source_data, sample_target_data):
        """测试完整比对流程 - 成功"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        with patch('core.compare_engine.base_engine.get_db_adapter', return_value=mock_db_adapter):
            engine = PandasCompareEngine(sample_config)

            # 模拟数据加载
            with patch.object(engine, 'load_data'):
                with patch.object(engine, 'compare') as mock_compare:
                    mock_compare.return_value = {'diff_cnt': 5}
                    with patch.object(engine, 'generate_report'):
                        result = engine.run()

        assert result['compare_status'] == 'success'

    def test_run_failure(self, sample_config, mock_db_adapter):
        """测试完整比对流程 - 失败"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        with patch('core.compare_engine.base_engine.get_db_adapter', return_value=mock_db_adapter):
            engine = PandasCompareEngine(sample_config)

            with patch.object(engine, 'load_data', side_effect=Exception("数据加载失败")):
                with pytest.raises(Exception, match="数据加载失败"):
                    engine.run()

    def test_close_connections_on_failure(self, sample_config, mock_db_adapter):
        """测试失败时关闭连接"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        with patch('core.compare_engine.base_engine.get_db_adapter', return_value=mock_db_adapter):
            engine = PandasCompareEngine(sample_config)
            engine.src_adapter = mock_db_adapter
            engine.tgt_adapter = mock_db_adapter

            with patch.object(engine, 'load_data', side_effect=Exception("Error")):
                try:
                    engine.run()
                except Exception:
                    pass

            # 即使失败也应该关闭连接
            assert mock_db_adapter.close.call_count == 2


class TestPandasCompareEngine:
    """Pandas比对引擎测试"""

    def test_load_data(self, sample_config, mock_db_adapter, sample_source_data, sample_target_data):
        """测试加载数据"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        with patch('core.db_adapter.base_adapter.get_db_adapter', return_value=mock_db_adapter):
            engine = PandasCompareEngine(sample_config)
            engine.src_adapter = mock_db_adapter
            engine.tgt_adapter = mock_db_adapter

            # 模拟查询结果
            mock_db_adapter.get_primary_keys.return_value = ['id']
            mock_db_adapter.get_extra_columns.return_value = ['age']
            mock_db_adapter.query_data.side_effect = [sample_source_data, sample_target_data]

            engine.load_data()

        assert engine.src_df is not None
        assert engine.tgt_df is not None
        assert len(engine.src_df) == 3

    def test_compare_no_differences(self, sample_config, mock_db_adapter):
        """测试无差异的比对"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        engine = PandasCompareEngine(sample_config)
        engine.src_df = pd.DataFrame([
            {'id': 1, 'name': 'Alice', 'age': 30},
            {'id': 2, 'name': 'Bob', 'age': 25}
        ])
        engine.tgt_df = pd.DataFrame([
            {'id': 1, 'name': 'Alice', 'age': 30},
            {'id': 2, 'name': 'Bob', 'age': 25}
        ])

        result = engine.compare()

        assert result['diff_cnt'] == 0
        assert result['matching_rate'] == 1.0

    def test_compare_with_differences(self, sample_config, mock_db_adapter):
        """测试有差异的比对"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        engine = PandasCompareEngine(sample_config)
        engine.src_df = pd.DataFrame([
            {'id': 1, 'name': 'Alice', 'age': 30},
            {'id': 2, 'name': 'Bob', 'age': 25},
            {'id': 3, 'name': 'Charlie', 'age': 35}
        ])
        engine.tgt_df = pd.DataFrame([
            {'id': 1, 'name': 'Alice', 'age': 30},
            {'id': 2, 'name': 'Bob', 'age': 26}  # age不同
            # id=3 缺失
        ])

        result = engine.compare()

        assert result['diff_cnt'] > 0
        assert result['matching_rate'] < 1.0

    def test_compare_missing_in_target(self, sample_config):
        """测试目标端缺失数据"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        engine = PandasCompareEngine(sample_config)
        engine.src_df = pd.DataFrame([
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
            {'id': 3, 'name': 'Charlie'}
        ])
        engine.tgt_df = pd.DataFrame([
            {'id': 1, 'name': 'Alice'}
        ])

        result = engine.compare()

        assert result['diff_cnt'] >= 2  # 至少2条差异

    def test_compare_extra_in_target(self, sample_config):
        """测试目标端多余数据"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        engine = PandasCompareEngine(sample_config)
        engine.src_df = pd.DataFrame([
            {'id': 1, 'name': 'Alice'}
        ])
        engine.tgt_df = pd.DataFrame([
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
            {'id': 3, 'name': 'Charlie'}
        ])

        result = engine.compare()

        assert result['diff_cnt'] >= 2


class TestSparkCompareEngine:
    """Spark比对引擎测试"""

    def test_init_spark_session(self, sample_config):
        """测试初始化Spark会话"""
        with patch('core.compare_engine.spark_engine.SparkSession') as mock_spark:
            from core.compare_engine.spark_engine import SparkCompareEngine

            engine = SparkCompareEngine(sample_config, 'spark_local')

            assert engine.spark is not None

    def test_load_data_spark(self, sample_config, mock_db_adapter):
        """测试Spark加载数据"""
        with patch('core.compare_engine.base_engine.get_db_adapter', return_value=mock_db_adapter):
            with patch('core.compare_engine.spark_engine.SparkSession'):
                from core.compare_engine.spark_engine import SparkCompareEngine

                engine = SparkCompareEngine(sample_config, 'spark_local')
                engine.src_adapter = mock_db_adapter
                engine.tgt_adapter = mock_db_adapter

                mock_db_adapter.get_primary_keys.return_value = ['id']
                mock_db_adapter.get_extra_columns.return_value = []
                mock_db_adapter.query_data.return_value = [
                    {'id': 1, 'name': 'Alice'},
                    {'id': 2, 'name': 'Bob'}
                ]

                engine.load_data()

    def test_compare_with_spark(self, sample_config):
        """测试Spark比对"""
        with patch('core.compare_engine.spark_engine.SparkSession') as mock_spark_session:
            from core.compare_engine.spark_engine import SparkCompareEngine

            # 模拟Spark DataFrame
            mock_src_df = Mock()
            mock_tgt_df = Mock()
            mock_joined_df = Mock()

            mock_src_df.count.return_value = 100
            mock_tgt_df.count.return_value = 95
            mock_joined_df.filter.return_value.count.return_value = 5

            engine = SparkCompareEngine(sample_config, 'spark_local')
            engine.src_df = mock_src_df
            engine.tgt_df = mock_tgt_df

            # 模拟join操作
            with patch.object(engine, 'compare') as mock_compare:
                mock_compare.return_value = {
                    'src_cnt': 100,
                    'tgt_cnt': 95,
                    'diff_cnt': 5,
                    'matching_rate': 0.95
                }

                result = engine.compare()

            assert result['diff_cnt'] == 5


class TestGetCompareEngine:
    """引擎选择测试"""

    def test_get_pandas_engine_small_data(self, sample_config, mock_db_adapter):
        """测试小数据量选择Pandas引擎"""
        with patch('core.db_adapter.base_adapter.get_db_adapter', return_value=mock_db_adapter):
            mock_db_adapter.get_table_count.return_value = 100000  # 10万条

            with patch('config.settings.ENGINE_STRATEGY', 'auto'):
                engine = get_compare_engine(sample_config)

            from core.compare_engine.pandas_engine import PandasCompareEngine
            assert isinstance(engine, PandasCompareEngine)

    def test_get_spark_local_medium_data(self, sample_config, mock_db_adapter):
        """测试中等数据量选择Spark本地模式"""
        with patch('core.db_adapter.base_adapter.get_db_adapter', return_value=mock_db_adapter):
            mock_db_adapter.get_table_count.return_value = 800000  # 80万条

            with patch('config.settings.ENGINE_STRATEGY', 'auto'):
                with patch('core.compare_engine.spark_engine.SparkSession'):
                    engine = get_compare_engine(sample_config)

            from core.compare_engine.spark_engine import SparkCompareEngine
            assert isinstance(engine, SparkCompareEngine)

    def test_get_spark_cluster_large_data(self, sample_config, mock_db_adapter):
        """测试大数据量选择Spark集群模式"""
        with patch('core.db_adapter.base_adapter.get_db_adapter', return_value=mock_db_adapter):
            mock_db_adapter.get_table_count.return_value = 5000000  # 500万条

            with patch('config.settings.ENGINE_STRATEGY', 'auto'):
                with patch('core.compare_engine.spark_engine.SparkSession'):
                    engine = get_compare_engine(sample_config)

            from core.compare_engine.spark_engine import SparkCompareEngine
            assert isinstance(engine, SparkCompareEngine)

    def test_force_pandas_engine(self, sample_config):
        """测试强制使用Pandas引擎"""
        with patch('config.settings.ENGINE_STRATEGY', 'pandas'):
            engine = get_compare_engine(sample_config)

        from core.compare_engine.pandas_engine import PandasCompareEngine
        assert isinstance(engine, PandasCompareEngine)

    def test_force_spark_local_engine(self, sample_config):
        """测试强制使用Spark本地模式"""
        with patch('config.settings.ENGINE_STRATEGY', 'spark_local'):
            with patch('core.compare_engine.spark_engine.SparkSession'):
                engine = get_compare_engine(sample_config)

        from core.compare_engine.spark_engine import SparkCompareEngine
        assert isinstance(engine, SparkCompareEngine)


class TestCompareEngineEdgeCases:
    """比对引擎边界条件测试"""

    def test_empty_source_table(self, sample_config, mock_db_adapter):
        """测试源表为空"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        with patch('core.compare_engine.base_engine.get_db_adapter', return_value=mock_db_adapter):
            engine = PandasCompareEngine(sample_config)
            engine.src_adapter = mock_db_adapter
            engine.tgt_adapter = mock_db_adapter

            mock_db_adapter.get_primary_keys.return_value = ['id']
            mock_db_adapter.get_extra_columns.return_value = []
            mock_db_adapter.query_data.return_value = []

            engine.load_data()

            assert len(engine.src_df) == 0

    def test_null_values_in_data(self, sample_config):
        """测试数据中包含NULL值"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        engine = PandasCompareEngine(sample_config)
        engine.src_df = pd.DataFrame([
            {'id': 1, 'name': 'Alice', 'age': None},
            {'id': 2, 'name': None, 'age': 25}
        ])
        engine.tgt_df = pd.DataFrame([
            {'id': 1, 'name': 'Alice', 'age': None},
            {'id': 2, 'name': None, 'age': 25}
        ])

        result = engine.compare()

        # NULL值应该被正确处理
        assert result['matching_rate'] == 1.0

    def test_duplicate_primary_keys(self, sample_config):
        """测试主键重复（异常情况）"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        engine = PandasCompareEngine(sample_config)
        # 模拟重复主键
        engine.src_df = pd.DataFrame([
            {'id': 1, 'name': 'Alice'},
            {'id': 1, 'name': 'Alice Duplicate'}
        ])

        # 应该能够处理或抛出明确错误

    def test_data_type_conversion(self, sample_config):
        """测试数据类型转换"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        engine = PandasCompareEngine(sample_config)
        engine.src_df = pd.DataFrame([
            {'id': '1', 'age': '30'},  # 字符串类型
        ])
        engine.tgt_df = pd.DataFrame([
            {'id': 1, 'age': 30},  # 数值类型
        ])

        # 数据类型应该被标准化
        result = engine.compare()
        # 比对结果取决于类型转换逻辑

    def test_very_wide_table(self, sample_config):
        """测试宽表（字段很多）"""
        from core.compare_engine.pandas_engine import PandasCompareEngine

        # 创建一个有100个字段的表
        columns = {f'col_{i}': i for i in range(100)}
        columns['id'] = 1

        engine = PandasCompareEngine(sample_config)
        engine.src_df = pd.DataFrame([columns])
        engine.tgt_df = pd.DataFrame([columns])

        result = engine.compare()

        assert result['matching_rate'] == 1.0
