#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 12:44
# @Author  : hejun
import requests
import json
from typing import List, Dict
from config.settings import WX_WORK_ROBOT, WX_AT_LIST, WX_ALERT_THRESHOLD


class WeChatNotification:
    """企业微信通知类"""

    def __init__(self):
        self.webhook_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={WX_WORK_ROBOT}"

    def send_alert(self, title: str, content: str, at_mobiles: List[str] = None):
        """发送企业微信告警"""
        at_mobiles = at_mobiles or WX_AT_LIST
        data = {
            "msgtype": "text",
            "text": {
                "content": f"{title}\n{content}",
                "mentioned_mobile_list": at_mobiles
            }
        }

        try:
            response = requests.post(
                self.webhook_url,
                data=json.dumps(data, ensure_ascii=False).encode('utf-8'),
                headers={"Content-Type": "application/json; charset=utf-8"},
                timeout=10
            )
            response.raise_for_status()
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"发送企业微信通知失败：{str(e)}")

    def send_compare_alert(self, config: Dict, compare_result: Dict):
        """发送比对告警"""
        # 检查是否需要告警
        compare_status = compare_result.get('compare_status')
        matching_rate = compare_result.get('matching_rate', 1.0)

        if compare_status == 'fail' or matching_rate < (1 - WX_ALERT_THRESHOLD):
            title = f"【数据一致性比对告警】{config['src_table_name']}"
            content = f"""
            表ID：{config.get('table_id')}
            源表：{config['src_db_name']}.{config['src_table_name']}
            目标表：{config['tgt_db_name']}.{config['tgt_table_name']}
            比对状态：{compare_result.get('compare_status')}
            差异记录数：{compare_result.get('diff_cnt')}
            匹配率：{matching_rate:.2%}
            错误信息：{compare_result.get('compare_msg', '')}
            """
            self.send_alert(title, content)

    def send_repair_alert(self, config: Dict, repair_result: Dict):
        """发送修复告警"""
        if repair_result.get('repair_status') == 'fail':
            title = f"【数据修复失败告警】{config['src_table_name']}"
            content = f"""
            表ID：{config.get('table_id')}
            源表：{config['src_db_name']}.{config['src_table_name']}
            目标表：{config['tgt_db_name']}.{config['tgt_table_name']}
            修复状态：{repair_result.get('repair_status')}
            修复记录数：{repair_result.get('repair_cnt', 0)}
            错误信息：{repair_result.get('repair_msg', '')}
            """
            self.send_alert(title, content)
