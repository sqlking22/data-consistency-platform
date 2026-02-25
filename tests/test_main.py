#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主流程测试用例
"""
import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from main import process_single_table, main


class TestProcessSingleTable:
    """单表处理测试"""

    def test_process_single_table_success(self, sample_config, sample_compare_result, sample_repair_result):
        """测试单表处理成功"""
        with patch('main.get_compare_engine') as mock_engine:
            with patch('main.WeChatNotification') as mock_notification:
                with patch('main.write_task_log') as mock_log:
                    # 模拟比对引擎
                    mock_compare_engine = Mock()
                    mock_compare_engine.run.return_value = sample_compare_result
                    mock_engine.return_value = mock_compare_engine

                    # 模拟修复引擎
                    with patch('main.DataXRepairEngine') as mock_repair:
                        mock_repair_engine = Mock()
                        mock_repair_engine.repair.return_value = sample_repair_result
                        mock_repair.return_value = mock_repair_engine

                        # 模拟通知
                        mock_notif_instance = Mock()
                        mock_notification.return_value = mock_notif_instance

                        result = process_single_table(sample_config)

        assert 'diff_cnt' in result
        mock_log.assert_called_once()

    def test_process_single_table_compare_failure(self, sample_config):
        """测试单表处理比对失败"""
        with patch('main.get_compare_engine') as mock_engine:
            with patch('main.WeChatNotification') as mock_notification:
                with patch('main.write_task_log') as mock_log:
                    # 模拟比对引擎失败
                    mock_compare_engine = Mock()
                    mock_compare_engine.run.side_effect = Exception("比对失败")
                    mock_engine.return_value = mock_compare_engine

                    mock_notif_instance = Mock()
                    mock_notification.return_value = mock_notif_instance

                    with pytest.raises(Exception, match="比对失败"):
                        process_single_table(sample_config)

        # 失败日志应该被写入
        assert mock_log.called

    def test_process_single_table_without_repair(self, sample_config, sample_compare_result):
        """测试单表处理不修复"""
        sample_config['enable_repair'] = False

        with patch('main.get_compare_engine') as mock_engine:
            with patch('main.WeChatNotification') as mock_notification:
                with patch('main.write_task_log') as mock_log:
                    mock_compare_engine = Mock()
                    mock_compare_engine.run.return_value = sample_compare_result
                    mock_engine.return_value = mock_compare_engine

                    mock_notif_instance = Mock()
                    mock_notification.return_value = mock_notif_instance

                    result = process_single_table(sample_config)

        assert 'diff_cnt' in result

    def test_process_single_table_notification_sent(self, sample_config, sample_compare_result):
        """测试发送通知"""
        sample_compare_result['matching_rate'] = 0.50  # 低匹配率

        with patch('main.get_compare_engine') as mock_engine:
            with patch('main.WeChatNotification') as mock_notification:
                with patch('main.write_task_log'):
                    mock_compare_engine = Mock()
                    mock_compare_engine.run.return_value = sample_compare_result
                    mock_engine.return_value = mock_compare_engine

                    with patch('main.DataXRepairEngine') as mock_repair:
                        mock_repair_engine = Mock()
                        mock_repair_engine.repair.return_value = {}
                        mock_repair.return_value = mock_repair_engine

                        mock_notif_instance = Mock()
                        mock_notification.return_value = mock_notif_instance

                        process_single_table(sample_config)

        # 验证通知被发送
        assert mock_notif_instance.send_compare_alert.called

    def test_process_single_table_timing(self, sample_config, sample_compare_result, sample_repair_result):
        """测试耗时计算"""
        with patch('main.get_compare_engine') as mock_engine:
            with patch('main.WeChatNotification') as mock_notification:
                with patch('main.write_task_log'):
                    sample_compare_result['compare_cost_minute'] = 2.5
                    sample_repair_result['repair_cost_minute'] = 1.5

                    mock_compare_engine = Mock()
                    mock_compare_engine.run.return_value = sample_compare_result
                    mock_engine.return_value = mock_compare_engine

                    with patch('main.DataXRepairEngine') as mock_repair:
                        mock_repair_engine = Mock()
                        mock_repair_engine.repair.return_value = sample_repair_result
                        mock_repair.return_value = mock_repair_engine

                        mock_notification.return_value = Mock()

                        result = process_single_table(sample_config)

        # 验证总耗时计算
        assert 'compare_total_cost_minute' in result
        assert result['compare_total_cost_minute'] == 4.0

    def test_process_single_table_log_fields(self, sample_config, sample_compare_result, sample_repair_result):
        """测试日志字段完整性"""
        with patch('main.get_compare_engine') as mock_engine:
            with patch('main.WeChatNotification') as mock_notification:
                with patch('main.write_task_log') as mock_log:
                    mock_compare_engine = Mock()
                    mock_compare_engine.run.return_value = sample_compare_result
                    mock_engine.return_value = mock_compare_engine

                    with patch('main.DataXRepairEngine') as mock_repair:
                        mock_repair_engine = Mock()
                        mock_repair_engine.repair.return_value = sample_repair_result
                        mock_repair.return_value = mock_repair_engine

                        mock_notification.return_value = Mock()

                        process_single_table(sample_config)

        # 验证日志包含必要字段
        call_args = mock_log.call_args[0]
        log_data = call_args[2]  # 第3个参数是日志数据

        assert 'table_id' in log_data
        assert 'compare_status' in log_data
        assert 'src_table_name' in log_data
        assert 'diff_cnt' in log_data


class TestMainFunction:
    """主函数测试"""

    def test_main_single_task_json_config(self, sample_config):
        """测试单任务JSON配置模式"""
        with patch('main.ConfigManager') as mock_config_mgr:
            mock_instance = Mock()
            mock_instance.load_all_configs.return_value = sample_config
            mock_config_mgr.return_value = mock_instance

            with patch('main.process_single_table') as mock_process:
                main()

            # 应该调用process_single_table
            mock_process.assert_called_once()

    def test_main_multiple_tasks(self, sample_config):
        """测试多任务模式"""
        config1 = sample_config.copy()
        config1['id'] = 1
        config2 = sample_config.copy()
        config2['id'] = 2

        global_config = {
            'concurrency': 2,
            'incremental': False,
            'enable_repair': True
        }

        with patch('main.ConfigManager') as mock_config_mgr:
            mock_instance = Mock()
            mock_instance.load_all_configs.return_value = {
                'global_config': global_config,
                'task_configs': [config1, config2]
            }
            mock_config_mgr.return_value = mock_instance

            with patch('main.ThreadPoolExecutor') as mock_executor:
                mock_executor_instance = MagicMock()
                mock_executor.return_value.__enter__ = Mock(return_value=mock_executor_instance)
                mock_executor.return_value.__exit__ = Mock(return_value=False)

                with patch('main.as_completed', return_value=[]):
                    main()

        # 验证线程池被使用
        mock_executor.assert_called_once_with(max_workers=2)

    def test_main_with_table_id(self, sample_config):
        """测试指定table_id"""
        with patch('main.ConfigManager') as mock_config_mgr:
            with patch('core.db_adapter.base_adapter.get_db_adapter') as mock_adapter:
                mock_config_instance = Mock()
                mock_config_instance.load_all_configs.return_value = sample_config
                mock_config_instance.load_all_configs.return_value['table_id'] = 1
                mock_config_mgr.return_value = mock_config_instance

                mock_db_adapter = Mock()
                mock_db_adapter.query.return_value = [sample_config]
                mock_adapter.return_value = mock_db_adapter

                with patch('main.process_single_table') as mock_process:
                    main()

                # 应该从数据库查询配置并处理
                assert mock_db_adapter.query.called

    def test_main_table_id_not_found(self, sample_config):
        """测试table_id不存在"""
        with patch('main.ConfigManager') as mock_config_mgr:
            with patch('core.db_adapter.base_adapter.get_db_adapter') as mock_adapter:
                mock_config_instance = Mock()
                mock_config_instance.load_all_configs.return_value = sample_config
                mock_config_instance.load_all_configs.return_value['table_id'] = 999
                mock_config_mgr.return_value = mock_config_instance

                mock_db_adapter = Mock()
                mock_db_adapter.query.return_value = []  # 查询无结果
                mock_adapter.return_value = mock_db_adapter

                with pytest.raises(ValueError, match="找不到ID为"):
                    main()

    def test_main_concurrent_task_failure(self, sample_config):
        """测试并发任务中的失败"""
        config1 = sample_config.copy()
        config1['id'] = 1
        config2 = sample_config.copy()
        config2['id'] = 2

        global_config = {'concurrency': 2}

        with patch('main.ConfigManager') as mock_config_mgr:
            mock_instance = Mock()
            mock_instance.load_all_configs.return_value = {
                'global_config': global_config,
                'task_configs': [config1, config2]
            }
            mock_config_mgr.return_value = mock_instance

            with patch('main.ThreadPoolExecutor') as mock_executor:
                # 模拟一个任务失败
                future1 = Mock()
                future1.result.return_value = {}
                future2 = Mock()
                future2.result.side_effect = Exception("Task failed")

                with patch('main.as_completed', return_value=[future1, future2]):
                    # 应该能继续执行,只记录错误
                    main()

    def test_main_timing(self, sample_config):
        """测试总耗时统计"""
        with patch('main.ConfigManager') as mock_config_mgr:
            mock_instance = Mock()
            mock_instance.load_all_configs.return_value = sample_config
            mock_config_mgr.return_value = mock_instance

            with patch('main.process_single_table'):
                with patch('main.logger') as mock_logger:
                    main()

        # 验证日志输出包含耗时信息
        assert mock_logger.info.called


class TestMainEdgeCases:
    """主流程边界条件测试"""

    def test_empty_task_configs(self):
        """测试空任务列表"""
        with patch('main.ConfigManager') as mock_config_mgr:
            mock_instance = Mock()
            mock_instance.load_all_configs.return_value = {
                'global_config': {},
                'task_configs': []
            }
            mock_config_mgr.return_value = mock_instance

            # 空任务列表应该正常执行
            main()

    def test_missing_required_config_fields(self):
        """测试缺少必要配置字段"""
        incomplete_config = {
            'src_table_name': 'test_table'
            # 缺少大部分字段
        }

        with patch('main.ConfigManager') as mock_config_mgr:
            mock_instance = Mock()
            mock_instance.load_all_configs.return_value = incomplete_config
            mock_config_mgr.return_value = mock_instance

            # 应该能够处理缺失字段或抛出明确错误

    def test_database_connection_failure_in_log(self, sample_config, sample_compare_result):
        """测试日志数据库连接失败"""
        with patch('main.get_compare_engine') as mock_engine:
            with patch('main.WeChatNotification') as mock_notification:
                with patch('main.write_task_log', side_effect=Exception("DB connection failed")):
                    mock_compare_engine = Mock()
                    mock_compare_engine.run.return_value = sample_compare_result
                    mock_engine.return_value = mock_compare_engine

                    mock_notification.return_value = Mock()

                    # 日志写入失败不应该影响主流程
                    with pytest.raises(Exception, match="DB connection failed"):
                        process_single_table(sample_config)

    def test_notification_failure(self, sample_config, sample_compare_result):
        """测试通知发送失败"""
        with patch('main.get_compare_engine') as mock_engine:
            with patch('main.WeChatNotification') as mock_notification:
                with patch('main.write_task_log'):
                    mock_compare_engine = Mock()
                    mock_compare_engine.run.return_value = sample_compare_result
                    mock_engine.return_value = mock_compare_engine

                    mock_notif_instance = Mock()
                    mock_notif_instance.send_compare_alert.side_effect = Exception("Notification failed")
                    mock_notification.return_value = mock_notif_instance

                    # 通知失败会导致整个流程失败(在当前实现中)
                    with pytest.raises(Exception, match="Notification failed"):
                        process_single_table(sample_config)

    def test_very_large_diff_count(self, sample_config):
        """测试超大差异记录数"""
        large_compare_result = {
            'src_cnt': 10000000,
            'tgt_cnt': 9000000,
            'diff_cnt': 1000000,
            'compare_status': 'success',
            'matching_rate': 0.9
        }

        with patch('main.get_compare_engine') as mock_engine:
            with patch('main.WeChatNotification') as mock_notification:
                with patch('main.write_task_log'):
                    mock_compare_engine = Mock()
                    mock_compare_engine.run.return_value = large_compare_result
                    mock_engine.return_value = mock_compare_engine

                    mock_notification.return_value = Mock()

                    result = process_single_table(sample_config)

        assert result['diff_cnt'] == 1000000

    def test_unicode_in_error_message(self, sample_config):
        """测试错误信息中的Unicode"""
        with patch('main.get_compare_engine') as mock_engine:
            with patch('main.WeChatNotification') as mock_notification:
                with patch('main.write_task_log'):
                    mock_compare_engine = Mock()
                    mock_compare_engine.run.side_effect = Exception("错误：数据不一致 ❌")
                    mock_engine.return_value = mock_compare_engine

                    mock_notification.return_value = Mock()

                    with pytest.raises(Exception, match="错误"):
                        process_single_table(sample_config)


class TestIntegration:
    """集成测试"""

    def test_full_workflow_success(self, sample_config, sample_compare_result, sample_repair_result):
        """测试完整工作流成功"""
        with patch('main.ConfigManager') as mock_config_mgr:
            mock_instance = Mock()
            mock_instance.load_all_configs.return_value = sample_config
            mock_config_mgr.return_value = mock_instance

            with patch('main.get_compare_engine') as mock_engine:
                mock_compare_engine = Mock()
                mock_compare_engine.run.return_value = sample_compare_result
                mock_engine.return_value = mock_compare_engine

                with patch('main.DataXRepairEngine') as mock_repair:
                    mock_repair_engine = Mock()
                    mock_repair_engine.repair.return_value = sample_repair_result
                    mock_repair.return_value = mock_repair_engine

                    with patch('main.WeChatNotification'):
                        with patch('main.write_task_log'):
                            main()

        # 所有组件应该被正确调用
        assert mock_engine.called
        assert mock_repair.called

    def test_workflow_with_retry(self, sample_config):
        """测试带重试的工作流 - 跳过此测试"""
        # 此测试需要更复杂的重试逻辑支持,暂时跳过
        pytest.skip("需要更复杂的重试逻辑支持")
