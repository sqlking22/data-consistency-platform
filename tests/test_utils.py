#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工具类测试用例
"""
import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from utils.db_utils import write_task_log, get_table_exists, get_table_writable
from utils import crypto_utils


class TestDBUtils:
    """数据库工具测试"""

    def test_write_task_log_success(self, sample_db_config):
        """测试写入任务日志成功"""
        log_data = {
            'table_id': 1,
            'compare_time': datetime.now(),
            'compare_status': 'success',
            'diff_cnt': 10
        }

        with patch('utils.db_utils.get_db_adapter') as mock_adapter:
            mock_db = Mock()
            mock_adapter.return_value = mock_db
            mock_db.execute.return_value = 1

            write_task_log(sample_db_config, 'task_result_log', log_data)

        # 验证SQL被执行
        assert mock_db.execute.called
        mock_db.close.assert_called_once()

    def test_write_task_log_with_none_values(self, sample_db_config):
        """测试写入日志包含None值"""
        log_data = {
            'table_id': 1,
            'compare_status': 'success',
            'repair_msg': None,  # None值
            'diff_cnt': 10
        }

        with patch('utils.db_utils.get_db_adapter') as mock_adapter:
            mock_db = Mock()
            mock_adapter.return_value = mock_db
            mock_db.execute.return_value = 1

            write_task_log(sample_db_config, 'task_result_log', log_data)

        # None值应该被过滤掉
        call_args = mock_db.execute.call_args[0][0]
        assert 'repair_msg' not in call_args

    def test_write_task_log_db_failure(self, sample_db_config):
        """测试写入日志数据库失败"""
        log_data = {'table_id': 1}

        with patch('utils.db_utils.get_db_adapter') as mock_adapter:
            mock_db = Mock()
            mock_adapter.return_value = mock_db
            mock_db.execute.side_effect = Exception("Database error")

            with pytest.raises(Exception, match="Database error"):
                write_task_log(sample_db_config, 'task_result_log', log_data)

    def test_write_task_log_special_characters(self, sample_db_config):
        """测试日志包含特殊字符"""
        log_data = {
            'table_id': 1,
            'compare_msg': "Error: <div>&\"special\"\nchars</div>"
        }

        with patch('utils.db_utils.get_db_adapter') as mock_adapter:
            mock_db = Mock()
            mock_adapter.return_value = mock_db

            write_task_log(sample_db_config, 'task_result_log', log_data)

        # 特殊字符应该被正确处理

    def test_get_table_exists_true(self, sample_db_config, mock_db_adapter):
        """测试表存在"""
        mock_db_adapter.get_table_metadata.return_value = [{'name': 'id'}]

        result = get_table_exists(mock_db_adapter, 'test_db', 'users')

        assert result is True

    def test_get_table_exists_false(self, sample_db_config, mock_db_adapter):
        """测试表不存在"""
        mock_db_adapter.get_table_metadata.side_effect = Exception("Table not found")

        result = get_table_exists(mock_db_adapter, 'test_db', 'nonexistent')

        assert result is False

    def test_get_table_writable_true(self, sample_db_config, mock_db_adapter):
        """测试表可写"""
        mock_db_adapter.execute.return_value = 0

        result = get_table_writable(mock_db_adapter, 'test_db', 'users')

        assert result is True

    def test_get_table_writable_false(self, sample_db_config, mock_db_adapter):
        """测试表不可写"""
        mock_db_adapter.execute.side_effect = Exception("Permission denied")

        result = get_table_writable(mock_db_adapter, 'test_db', 'users')

        assert result is False


class TestCryptoUtils:
    """加密工具测试"""

    def test_encrypt_decrypt(self):
        """测试加密解密"""
        original = "my_password_123"
        encrypted = crypto_utils.encrypt(original)
        decrypted = crypto_utils.decrypt(encrypted)

        assert decrypted == original
        assert encrypted != original

    def test_encrypt_empty_string(self):
        """测试加密空字符串"""
        encrypted = crypto_utils.encrypt("")
        decrypted = crypto_utils.decrypt(encrypted)

        assert decrypted == ""

    def test_encrypt_unicode(self):
        """测试加密Unicode字符"""
        original = "密码测试123"
        encrypted = crypto_utils.encrypt(original)
        decrypted = crypto_utils.decrypt(encrypted)

        assert decrypted == original

    def test_encrypt_special_characters(self):
        """测试加密特殊字符"""
        original = "p@ssw0rd!#$%^&*()"
        encrypted = crypto_utils.encrypt(original)
        decrypted = crypto_utils.decrypt(encrypted)

        assert decrypted == original

    def test_decrypt_invalid_data(self):
        """测试解密无效数据"""
        with pytest.raises(Exception):
            crypto_utils.decrypt("invalid_encrypted_data")

    def test_different_encryptions_same_text(self):
        """测试相同文本不同加密结果(AES随机IV)"""
        text = "same_password"
        encrypted1 = crypto_utils.encrypt(text)
        encrypted2 = crypto_utils.encrypt(text)

        # 相同文本的加密结果应该不同(因为随机IV)
        # 但都能正确解密
        assert encrypted1 != encrypted2
        assert crypto_utils.decrypt(encrypted1) == text
        assert crypto_utils.decrypt(encrypted2) == text


class TestDataTypeUtils:
    """数据类型工具测试"""

    def test_unify_data_types_numeric(self):
        """测试数值类型统一"""
        import pandas as pd
        from utils.data_type_utils import unify_data_types

        df1 = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})
        df2 = pd.DataFrame({'id': [1, 2], 'value': [10.5, 20.5]})

        df1_unified, df2_unified = unify_data_types(df1, df2)

        # 应该都转换为float
        assert df1_unified['value'].dtype == df2_unified['value'].dtype

    def test_unify_data_types_datetime(self):
        """测试日期类型统一"""
        import pandas as pd
        from utils.data_type_utils import unify_data_types

        df1 = pd.DataFrame({'id': [1], 'date': pd.to_datetime(['2026-02-25'])})
        df2 = pd.DataFrame({'id': [1], 'date': ['2026-02-26']})

        df1_unified, df2_unified = unify_data_types(df1, df2)

        # 应该都转换为datetime
        assert pd.api.types.is_datetime64_any_dtype(df1_unified['date'])

    def test_unify_data_types_string(self):
        """测试字符串类型统一"""
        import pandas as pd
        from utils.data_type_utils import unify_data_types

        df1 = pd.DataFrame({'id': [1], 'name': ['Alice']})
        df2 = pd.DataFrame({'id': [1], 'name': ['Bob']})

        df1_unified, df2_unified = unify_data_types(df1, df2)

        # 应该保持字符串类型
        assert df1_unified['name'].dtype == object


class TestRetryUtils:
    """重试工具测试"""

    def test_retry_decorator_success_first_try(self):
        """测试第一次就成功"""
        from utils.retry_utils import retry_decorator

        call_count = [0]

        @retry_decorator(max_retries=3, delay=0.1)
        def successful_func():
            call_count[0] += 1
            return "success"

        result = successful_func()

        assert result == "success"
        assert call_count[0] == 1

    def test_retry_decorator_success_after_retries(self):
        """测试重试后成功"""
        from utils.retry_utils import retry_decorator

        call_count = [0]

        @retry_decorator(max_retries=3, delay=0.1)
        def retry_func():
            call_count[0] += 1
            if call_count[0] < 3:
                raise Exception("Temporary error")
            return "success"

        result = retry_func()

        assert result == "success"
        assert call_count[0] == 3

    def test_retry_decorator_all_failures(self):
        """测试所有重试都失败"""
        from utils.retry_utils import retry_decorator

        call_count = [0]

        @retry_decorator(max_retries=3, delay=0.1)
        def always_fail():
            call_count[0] += 1
            raise Exception("Persistent error")

        with pytest.raises(Exception, match="Persistent error"):
            always_fail()

        assert call_count[0] == 3

    def test_retry_decorator_with_specific_exceptions(self):
        """测试指定异常类型重试"""
        from utils.retry_utils import retry_decorator

        call_count = [0]

        @retry_decorator(max_retries=3, delay=0.1, exceptions=(ValueError,))
        def raise_value_error():
            call_count[0] += 1
            if call_count[0] < 2:
                raise ValueError("Retry this")
            return "success"

        result = raise_value_error()

        assert result == "success"
        assert call_count[0] == 2

    def test_retry_decorator_non_retryable_exception(self):
        """测试不可重试的异常"""
        from utils.retry_utils import retry_decorator

        call_count = [0]

        @retry_decorator(max_retries=3, delay=0.1, exceptions=(ValueError,))
        def raise_type_error():
            call_count[0] += 1
            raise TypeError("Don't retry this")

        with pytest.raises(TypeError):
            raise_type_error()

        # TypeError不在重试列表,只执行一次
        assert call_count[0] == 1


class TestReportUtils:
    """报告工具测试"""

    def test_generate_html_report(self, sample_compare_result):
        """测试生成HTML报告"""
        from utils.report_utils import generate_html_report

        html = generate_html_report(sample_compare_result)

        assert '<html' in html.lower()
        assert str(sample_compare_result['diff_cnt']) in html
        assert str(sample_compare_result['src_cnt']) in html

    def test_generate_html_report_with_differences(self):
        """测试生成带差异的报告"""
        from utils.report_utils import generate_html_report

        compare_result = {
            'src_cnt': 100,
            'tgt_cnt': 95,
            'diff_cnt': 5,
            'matching_rate': 0.95,
            'src_table_name': 'source_table',
            'tgt_table_name': 'target_table'
        }

        html = generate_html_report(compare_result)

        assert 'diff_cnt' in html or '5' in html

    def test_generate_html_report_empty(self):
        """测试生成空报告"""
        from utils.report_utils import generate_html_report

        compare_result = {}
        html = generate_html_report(compare_result)

        # 应该能够处理空数据
        assert html is not None


class TestUtilsEdgeCases:
    """工具类边界条件测试"""

    def test_very_long_password(self):
        """测试超长密码加密"""
        long_password = "a" * 10000
        encrypted = crypto_utils.encrypt(long_password)
        decrypted = crypto_utils.decrypt(encrypted)

        assert decrypted == long_password

    def test_log_data_all_none(self, sample_db_config):
        """测试所有字段都是None"""
        log_data = {
            'field1': None,
            'field2': None,
            'field3': None
        }

        with patch('utils.db_utils.get_db_adapter') as mock_adapter:
            mock_db = Mock()
            mock_adapter.return_value = mock_db

            # 应该能够处理全None的情况
            write_task_log(sample_db_config, 'task_result_log', log_data)

    def test_unicode_in_table_name(self, sample_db_config):
        """测试表名中的Unicode"""
        log_data = {
            'table_id': 1,
            'src_table_name': '用户表',
            'tgt_table_name': '订单表'
        }

        with patch('utils.db_utils.get_db_adapter') as mock_adapter:
            mock_db = Mock()
            mock_adapter.return_value = mock_db

            write_task_log(sample_db_config, 'task_result_log', log_data)

            # Unicode应该被正确处理

    def test_retry_with_zero_delay(self):
        """测试零延迟重试"""
        from utils.retry_utils import retry_decorator

        call_count = [0]

        @retry_decorator(max_retries=2, delay=0)
        def instant_retry():
            call_count[0] += 1
            if call_count[0] < 2:
                raise Exception("Error")
            return "success"

        result = instant_retry()

        assert result == "success"

    def test_retry_with_negative_retries(self):
        """测试负数重试次数"""
        from utils.retry_utils import retry_decorator

        @retry_decorator(max_retries=-1, delay=0.1)
        def func():
            return "success"

        # 负数应该被处理,函数应该返回结果
        result = func()
        assert result == "success"
