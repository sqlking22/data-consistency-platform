#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通知模块测试用例
"""
import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from core.notification import WeChatNotification


class TestWeChatNotification:
    """企业微信通知测试"""

    def test_init(self):
        """测试初���化"""
        notification = WeChatNotification()

        assert 'qyapi.weixin.qq.com' in notification.webhook_url
        # Just check that webhook_url contains the key pattern
        assert 'key=' in notification.webhook_url

    def test_send_alert_success(self, mock_requests_post):
        """测试发送告警成功"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            notification.send_alert("Test Title", "Test Content")

            # 验证请求被调用
            mock_requests_post.assert_called_once()

            # 验证请求参数
            call_args = mock_requests_post.call_args
            assert 'data' in call_args.kwargs
            assert 'headers' in call_args.kwargs

    def test_send_alert_with_at_mobiles(self, mock_requests_post):
        """测试发送告警带@手机号"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            notification.send_alert("Title", "Content", at_mobiles=['13800138000', '13900139000'])

            # 验证@列表包含在请求中
            call_args = mock_requests_post.call_args
            sent_data = json.loads(call_args.kwargs['data'])
            assert 'mentioned_mobile_list' in sent_data['text']

    def test_send_alert_network_error(self):
        """测试发送告警网络错误"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            with patch('requests.post', side_effect=Exception("Network error")):
                with patch('logging.getLogger') as mock_logger:
                    # 不应该抛出异常,只记录日志
                    notification.send_alert("Title", "Content")

    def test_send_alert_timeout(self):
        """测试发送告警超时"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            with patch('requests.post') as mock_post:
                import requests
                mock_post.side_effect = requests.Timeout("Request timeout")

                with patch('logging.getLogger'):
                    # 不应该抛出异常
                    notification.send_alert("Title", "Content")

    def test_send_compare_alert_success(self, sample_config):
        """测试发送比对成功告警"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            with patch('config.settings.WX_ALERT_THRESHOLD', 0.05):
                notification = WeChatNotification()

                compare_result = {
                    'compare_status': 'success',
                    'diff_cnt': 0,
                    'matching_rate': 1.0,
                    'compare_msg': ''
                }

                with patch.object(notification, 'send_alert') as mock_alert:
                    notification.send_compare_alert(sample_config, compare_result)

                # 匹配率100%,不应该发送告警
                mock_alert.assert_not_called()

    def test_send_compare_alert_with_differences(self, sample_config):
        """测试发送比对差异告警"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            with patch('config.settings.WX_ALERT_THRESHOLD', 0.05):
                notification = WeChatNotification()

                compare_result = {
                    'compare_status': 'success',
                    'diff_cnt': 100,
                    'matching_rate': 0.90,  # 匹配率90%,差异率10% > 阈值5%
                    'compare_msg': '发现差异'
                }

                with patch.object(notification, 'send_alert') as mock_alert:
                    notification.send_compare_alert(sample_config, compare_result)

                # 差异率超过阈值,应该发送告警
                mock_alert.assert_called_once()

                # 验证告警内容
                call_args = mock_alert.call_args
                assert '数据一致性比对告警' in call_args[0][0]

    def test_send_compare_alert_failure(self, sample_config):
        """测试发送比对失败告警"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            compare_result = {
                'compare_status': 'fail',
                'diff_cnt': 0,
                'matching_rate': 0.0,
                'compare_msg': '数据库连接失败'
            }

            with patch.object(notification, 'send_alert') as mock_alert:
                notification.send_compare_alert(sample_config, compare_result)

            # 失败状态应该发送告警
            mock_alert.assert_called_once()

    def test_send_compare_alert_below_threshold(self, sample_config):
        """测试差异率低于阈值不告警"""
        with patch('config.settings.WX_ALERT_THRESHOLD', 0.10):  # 阈值10%
            notification = WeChatNotification()

            compare_result = {
                'compare_status': 'success',
                'diff_cnt': 5,
                'matching_rate': 0.96,  # 差异率4% < 阈值10%
                'compare_msg': ''
            }

            with patch.object(notification, 'send_alert') as mock_alert:
                notification.send_compare_alert(sample_config, compare_result)

            # 差异率低于阈值,不应该发送告警
            mock_alert.assert_not_called()

    def test_send_repair_alert_success(self, sample_config, sample_repair_result):
        """测试发送修复成功告警"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            repair_result = sample_repair_result.copy()
            repair_result['repair_status'] = 'success'

            with patch.object(notification, 'send_alert') as mock_alert:
                notification.send_repair_alert(sample_config, repair_result)

            # 修复成功不应该发送告警
            mock_alert.assert_not_called()

    def test_send_repair_alert_failure(self, sample_config):
        """测试发送修复失败告警"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            repair_result = {
                'repair_status': 'fail',
                'repair_cnt': 0,
                'repair_msg': 'DataX执行失败'
            }

            with patch.object(notification, 'send_alert') as mock_alert:
                notification.send_repair_alert(sample_config, repair_result)

            # 修复失败应该发送告警
            mock_alert.assert_called_once()

            # 验证告警内容
            call_args = mock_alert.call_args
            assert '数据修复失败告警' in call_args[0][0]

    def test_send_repair_alert_skip(self, sample_config):
        """测试修复跳过不告警"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            repair_result = {
                'repair_status': 'skip',
                'repair_cnt': 0,
                'repair_msg': '无差异记录，无需修复'
            }

            with patch.object(notification, 'send_alert') as mock_alert:
                notification.send_repair_alert(sample_config, repair_result)

            # 跳过状态不应该发送告警
            mock_alert.assert_not_called()


class TestNotificationContent:
    """通知内容测试"""

    def test_alert_content_format(self, sample_config, mock_requests_post):
        """测试告警内容格式"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            compare_result = {
                'compare_status': 'fail',
                'diff_cnt': 50,
                'matching_rate': 0.95,
                'compare_msg': 'Test error'
            }

            notification.send_compare_alert(sample_config, compare_result)

            # 验证发送的内容
            call_args = mock_requests_post.call_args
            sent_data = json.loads(call_args.kwargs['data'])

            assert sent_data['msgtype'] == 'text'
            assert 'content' in sent_data['text']

            content = sent_data['text']['content']
            assert sample_config['src_table_name'] in content
            assert sample_config['tgt_table_name'] in content
            assert '50' in content  # diff_cnt

    def test_unicode_content(self, sample_config, mock_requests_post):
        """测试Unicode内容"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            sample_config['src_table_name'] = '用户表'
            sample_config['tgt_table_name'] = '订单表'

            compare_result = {
                'compare_status': 'fail',
                'diff_cnt': 10,
                'matching_rate': 0.0,
                'compare_msg': '比对失败：数据不一致'
            }

            notification.send_compare_alert(sample_config, compare_result)

            # 验证Unicode字符被正确处理
            call_args = mock_requests_post.call_args
            sent_data = json.loads(call_args.kwargs['data'])
            content = sent_data['text']['content']

            assert '用户表' in content
            assert '订单表' in content

    def test_special_characters_in_content(self, sample_config, mock_requests_post):
        """测试内容中的特殊字符"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            compare_result = {
                'compare_status': 'fail',
                'diff_cnt': 10,
                'matching_rate': 0.0,
                'compare_msg': 'Error: <div>&"special"\nchars</div>'
            }

            notification.send_compare_alert(sample_config, compare_result)

            # 特殊字符应该被正确处理
            call_args = mock_requests_post.call_args
            assert call_args is not None


class TestNotificationEdgeCases:
    """通知边界条件测试"""

    def test_empty_at_list(self, mock_requests_post):
        """测试空@列表"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            with patch('config.settings.WX_AT_LIST', []):
                notification = WeChatNotification()

                notification.send_alert("Title", "Content")

                # 空列表应该被正确处理

    def test_none_at_mobiles(self, mock_requests_post):
        """测试None的@列表"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            notification.send_alert("Title", "Content", at_mobiles=None)

            # None应该使用默认值

    def test_very_long_content(self, sample_config, mock_requests_post):
        """测试超长内容"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            long_msg = "A" * 10000
            compare_result = {
                'compare_status': 'fail',
                'diff_cnt': 10,
                'matching_rate': 0.0,
                'compare_msg': long_msg
            }

            notification.send_compare_alert(sample_config, compare_result)

            # 超长内容应该被发送(实际可能被微信截断)

    def test_concurrent_alerts(self, sample_config):
        """测试并发发送告警"""
        import threading

        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            compare_result = {
                'compare_status': 'fail',
                'diff_cnt': 10,
                'matching_rate': 0.0,
                'compare_msg': 'Test'
            }

            threads = []
            for i in range(10):
                t = threading.Thread(
                    target=notification.send_compare_alert,
                    args=(sample_config, compare_result)
                )
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            # 并发发送应该都能完成

    def test_invalid_webhook_url(self):
        """测试无效的webhook URL"""
        with patch('config.settings.WX_WORK_ROBOT', ''):
            notification = WeChatNotification()

            # webhook_url可能无效但不应该崩溃
            assert notification.webhook_url is not None

    def test_rate_limiting(self, sample_config, mock_requests_post):
        """测试频率限制(模拟)"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            compare_result = {
                'compare_status': 'fail',
                'diff_cnt': 10,
                'matching_rate': 0.0,
                'compare_msg': 'Test'
            }

            # 快速发送多次告警
            for _ in range(100):
                notification.send_compare_alert(sample_config, compare_result)

            # 实际中可能被限流,但测试中只验证不崩溃

    def test_http_status_code_error(self):
        """测试HTTP状态码错误"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            notification = WeChatNotification()

            with patch('requests.post') as mock_post:
                mock_response = Mock()
                mock_response.status_code = 500
                mock_response.raise_for_status.side_effect = Exception("Server error")
                mock_post.return_value = mock_response

                with patch('logging.getLogger'):
                    # 不应该抛出异常
                    notification.send_alert("Title", "Content")

    def test_missing_config_fields(self, mock_requests_post):
        """测试缺少配置字段"""
        notification = WeChatNotification()

        incomplete_config = {
            'src_table_name': 'test_table',
            'src_db_name': 'test_db',
            'tgt_table_name': 'target_table',
            'tgt_db_name': 'target_db',
            'table_id': 1
        }

        compare_result = {
            'compare_status': 'fail',
            'diff_cnt': 10,
            'matching_rate': 0.0,
            'compare_msg': 'Test'
        }

        # 应该能够处理缺失字段
        notification.send_compare_alert(incomplete_config, compare_result)


class TestNotificationIntegration:
    """通知集成测试"""

    def test_full_notification_workflow(self, sample_config, sample_compare_result, sample_repair_result):
        """测试完整通知工作流"""
        with patch('config.settings.WX_WORK_ROBOT', 'test-key'):
            with patch('config.settings.WX_ALERT_THRESHOLD', 0.00):
                notification = WeChatNotification()

                with patch.object(notification, 'send_alert') as mock_alert:
                    # 发送比对告警
                    notification.send_compare_alert(sample_config, sample_compare_result)

                    # 发送修复告警
                    sample_repair_result['repair_status'] = 'fail'
                    notification.send_repair_alert(sample_config, sample_repair_result)

                # 验证两次告警都被发送
                assert mock_alert.call_count == 2
