#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置管理器测试用例
"""
import pytest
import os
import json
from unittest.mock import Mock, patch, MagicMock
from core.config_manager import ConfigManager
from config.settings import (
    MAX_THREAD_COUNT, ENABLE_REPAIR, IS_INCREMENTAL,
    INCREMENTAL_DAYS, DECODE_PASSWORD_FLAG
)


class TestConfigManager:
    """配置管理器测试类"""

    def test_init(self):
        """测试初始化"""
        manager = ConfigManager()
        assert manager.global_config == {}
        assert manager.task_config == {}
        assert manager.args is None

    def test_load_global_config_defaults(self):
        """测试加载全局配置 - 默认值"""
        manager = ConfigManager()
        with patch.object(manager, 'load_cli_args'):
            manager.load_global_config()

        assert 'concurrency' in manager.global_config
        assert 'incremental' in manager.global_config
        assert 'enable_repair' in manager.global_config
        assert manager.global_config['time_tolerance'] == 300
        assert manager.global_config['retry_times'] == 3

    def test_load_global_config_with_cli_args(self):
        """测试加载全局配置 - 命令行参数优先"""
        manager = ConfigManager()

        # 模拟命令行参数
        with patch.object(manager, 'load_cli_args'):
            manager.args = Mock()
            manager.args.concurrency = 10
            manager.args.incremental = True
            manager.args.incremental_days = 5
            manager.args.enable_repair = False
            manager.args.config_file = 'custom_config.json'

            manager.load_global_config()

        assert manager.global_config['concurrency'] == 10
        assert manager.global_config['incremental'] is True
        assert manager.global_config['incremental_days'] == 5
        assert manager.global_config['enable_repair'] is False
        assert manager.global_config['config_file'] == 'custom_config.json'

    def test_load_json_config_if_exists_valid(self, temp_config_file):
        """测试加载有效JSON配置"""
        manager = ConfigManager()
        result = manager.load_json_config_if_exists(temp_config_file)

        assert result is True
        assert manager.task_config['src_db_type'] == 'mysql'
        assert manager.task_config['src_table_name'] == 'source_table'

    def test_load_json_config_if_exists_not_found(self):
        """测试加载不存在的JSON配置"""
        manager = ConfigManager()
        result = manager.load_json_config_if_exists('/nonexistent/config.json')

        assert result is False
        assert manager.task_config == {}

    def test_load_json_config_if_exists_missing_fields(self, tmp_path):
        """测试加载缺少必要字段的JSON配置"""
        manager = ConfigManager()

        # 创建缺少字段的配置文件
        invalid_config = {"src_db_type": "mysql"}  # 缺少大部分必要字段
        config_file = tmp_path / "invalid_config.json"
        with open(config_file, 'w') as f:
            json.dump(invalid_config, f)

        result = manager.load_json_config_if_exists(str(config_file))

        assert result is False

    def test_load_json_config_if_exists_invalid_json(self, tmp_path):
        """测试加载无效的JSON文件"""
        manager = ConfigManager()

        config_file = tmp_path / "invalid.json"
        with open(config_file, 'w') as f:
            f.write("not a valid json {{{")

        result = manager.load_json_config_if_exists(str(config_file))

        assert result is False

    def test_load_db_config_single_task(self, sample_config, mock_db_adapter):
        """测试从数据库加载单个任务配置"""
        manager = ConfigManager()
        manager.global_config['table_id'] = 1

        with patch('core.config_manager.get_db_adapter', return_value=mock_db_adapter):
            mock_db_adapter.query.return_value = [sample_config]
            manager.load_db_config()

        assert manager.task_config['src_table_name'] == 'source_table'
        mock_db_adapter.close.assert_called_once()

    def test_load_db_config_multiple_tasks(self, sample_config, mock_db_adapter):
        """测试从数据库加载多个任务配置"""
        manager = ConfigManager()
        # 不指定table_id,加载所有任务

        config1 = sample_config.copy()
        config1['id'] = 1
        config2 = sample_config.copy()
        config2['id'] = 2
        config2['src_table_name'] = 'source_table2'

        with patch('core.config_manager.get_db_adapter', return_value=mock_db_adapter):
            mock_db_adapter.query.return_value = [config1, config2]
            manager.load_db_config()

        assert isinstance(manager.task_config, list)
        assert len(manager.task_config) == 2
        assert manager.task_config[1]['src_table_name'] == 'source_table2'

    def test_load_db_config_no_result(self, mock_db_adapter):
        """测试数据库查询无结果"""
        manager = ConfigManager()
        manager.global_config['table_id'] = 999

        with patch('core.config_manager.get_db_adapter', return_value=mock_db_adapter):
            mock_db_adapter.query.return_value = []
            manager.load_db_config()

        # 无结果时应该保持空字典(单任务模式)或空列表(多任务模式)
        assert manager.task_config in ([], {})

    def test_decrypt_password_enabled(self, sample_config):
        """测试解密密码 - 启用解密"""
        manager = ConfigManager()
        manager.task_config = sample_config.copy()
        manager.task_config['src_password'] = 'encrypted_password'
        manager.task_config['tgt_password'] = 'encrypted_password2'

        with patch('config.settings.DECODE_PASSWORD_FLAG', True):
            with patch('utils.crypto_utils.decrypt', side_effect=['decrypted1', 'decrypted2']):
                manager.decrypt_password()

        assert manager.task_config['src_password'] == 'decrypted1'
        assert manager.task_config['tgt_password'] == 'decrypted2'

    def test_decrypt_password_disabled(self, sample_config):
        """测试解密密码 - 禁用解密"""
        manager = ConfigManager()
        manager.task_config = sample_config.copy()
        original_src_pwd = manager.task_config['src_password']
        original_tgt_pwd = manager.task_config['tgt_password']

        with patch('config.settings.DECODE_PASSWORD_FLAG', False):
            manager.decrypt_password()

        # 密码应该保持不变
        assert manager.task_config['src_password'] == original_src_pwd
        assert manager.task_config['tgt_password'] == original_tgt_pwd

    def test_load_all_configs_single_task(self, temp_config_file):
        """测试加载所有配置 - 单任务模式"""
        manager = ConfigManager()

        with patch.object(manager, 'load_cli_args'):
            manager.args = Mock()
            manager.args.config_file = temp_config_file
            manager.args.concurrency = None
            manager.args.incremental = None
            manager.args.incremental_days = None
            manager.args.enable_repair = None

            final_config = manager.load_all_configs()

        assert 'src_db_type' in final_config
        assert 'concurrency' in final_config
        assert final_config['src_table_name'] == 'source_table'

    def test_load_all_configs_multiple_tasks(self, sample_config, mock_db_adapter):
        """测试加载所有配置 - 多任务模式"""
        manager = ConfigManager()

        config1 = sample_config.copy()
        config2 = sample_config.copy()
        config2['id'] = 2

        with patch.object(manager, 'load_cli_args'):
            manager.args = Mock()
            manager.args.config_file = 'nonexistent.json'  # JSON不存在,从数据库加载
            manager.args.concurrency = None
            manager.args.incremental = None
            manager.args.incremental_days = None
            manager.args.enable_repair = None

            with patch('core.config_manager.get_db_adapter', return_value=mock_db_adapter):
                mock_db_adapter.query.return_value = [config1, config2]
                result = manager.load_all_configs()

        assert 'global_config' in result
        assert 'task_configs' in result
        assert len(result['task_configs']) == 2

    def test_config_priority_cli_over_settings(self):
        """测试配置优先级: 命令行 > settings.py"""
        manager = ConfigManager()

        with patch.object(manager, 'load_cli_args'):
            manager.args = Mock()
            manager.args.concurrency = 99
            manager.args.incremental = True
            manager.args.incremental_days = 10
            manager.args.enable_repair = False
            manager.args.config_file = 'test.json'

            manager.load_global_config()

        # 命令行参数应该覆盖settings.py
        assert manager.global_config['concurrency'] == 99
        assert manager.global_config['incremental'] is True
        assert manager.global_config['incremental_days'] == 10
        assert manager.global_config['enable_repair'] is False

    def test_config_priority_json_over_db(self, temp_config_file, mock_db_adapter):
        """测试配置优先级: JSON > 数据库"""
        manager = ConfigManager()

        with patch.object(manager, 'load_cli_args'):
            manager.args = Mock()
            manager.args.config_file = temp_config_file
            manager.args.concurrency = None
            manager.args.incremental = None
            manager.args.incremental_days = None
            manager.args.enable_repair = None

            manager.load_all_configs()

        # 应该加载JSON配置,而不是数据库
        assert manager.task_config['src_table_name'] == 'source_table'
        mock_db_adapter.query.assert_not_called()


class TestConfigManagerEdgeCases:
    """配置管理器边界条件测试"""

    def test_empty_password_decrypt(self):
        """测试空密码解密"""
        manager = ConfigManager()
        manager.task_config = {
            'src_password': '',
            'tgt_password': None
        }

        with patch('config.settings.DECODE_PASSWORD_FLAG', True):
            # 不应该抛出异常
            manager.decrypt_password()

    def test_missing_password_fields(self):
        """测试缺少密码字段"""
        manager = ConfigManager()
        manager.task_config = {}

        with patch('config.settings.DECODE_PASSWORD_FLAG', True):
            # 不应该抛出异常
            manager.decrypt_password()

    def test_cli_args_none_values(self):
        """测试命令行参数为None时使用默认值"""
        manager = ConfigManager()

        with patch.object(manager, 'load_cli_args'):
            manager.args = Mock()
            manager.args.concurrency = None
            manager.args.incremental = None
            manager.args.incremental_days = None
            manager.args.enable_repair = None
            manager.args.config_file = None

            manager.load_global_config()

        # None值应该被settings.py默认值替换
        assert manager.global_config['concurrency'] == MAX_THREAD_COUNT
        assert manager.global_config['incremental'] == IS_INCREMENTAL
        assert manager.global_config['incremental_days'] == INCREMENTAL_DAYS

    def test_db_connection_failure(self, mock_db_adapter):
        """测试数据库连接失败"""
        manager = ConfigManager()
        manager.global_config['table_id'] = 1

        with patch('core.config_manager.get_db_adapter', return_value=mock_db_adapter):
            mock_db_adapter.query.side_effect = Exception("Database connection failed")

            with pytest.raises(Exception):
                manager.load_db_config()

    def test_json_file_permission_denied(self, tmp_path):
        """测试JSON文件权限不足"""
        manager = ConfigManager()
        config_file = tmp_path / "config.json"

        # 创建文件
        with open(config_file, 'w') as f:
            json.dump({"test": "data"}, f)

        # 模拟权限错误
        with patch('builtins.open', side_effect=PermissionError("Permission denied")):
            result = manager.load_json_config_if_exists(str(config_file))

        assert result is False
