#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 16:15
# @Author  : hejun

import logging
from typing import Dict, Any
from jinja2 import Template

logger = logging.getLogger(__name__)


def generate_html_report(compare_result: Dict[str, Any]) -> str:
    """生成HTML格式的比对报告"""
    # HTML模板
    html_template = """
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>数据一致性比对报告</title>
        <style>
            body {font-family: Arial, sans-serif; margin: 20px;}
            .header {background-color: #f0f0f0; padding: 10px; border-radius: 5px;}
            .metrics {display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; margin: 20px 0;}
            .metric-card {border: 1px solid #ddd; padding: 15px; border-radius: 5px;}
            .report {background-color: #f9f9f9; padding: 15px; border-radius: 5px; white-space: pre-wrap;}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>数据一致性比对报告</h2>
            <p>比对时间: {{ compare_time }}</p>
        </div>
        <div class="metrics">
            <div class="metric-card">
                <h4>源端记录数</h4>
                <p>{{ src_cnt }}</p>
            </div>
            <div class="metric-card">
                <h4>目标端记录数</h4>
                <p>{{ tgt_cnt }}</p>
            </div>
            <div class="metric-card">
                <h4>差异记录数</h4>
                <p>{{ diff_cnt }}</p>
            </div>
            <div class="metric-card">
                <h4>匹配率</h4>
                <p>{{ matching_rate }}</p>
            </div>
            <div class="metric-card">
                <h4>比对耗时</h4>
                <p>{{ compare_cost_minute }} 分钟</p>
            </div>
            <div class="metric-card">
                <h4>比对状态</h4>
                <p>{{ compare_status }}</p>
            </div>
        </div>
        <div class="report">
            <h4>详细比对结果</h4>
            <p>{{ compare_report }}</p>
        </div>
    </body>
    </html>
    """

    try:
        template = Template(html_template)
        # 预先格式化数据
        formatted_result = {
            'compare_time': compare_result.get('compare_time', ''),
            'src_cnt': compare_result.get('src_cnt', 0),
            'tgt_cnt': compare_result.get('tgt_cnt', 0),
            'diff_cnt': compare_result.get('diff_cnt', 0),
            'matching_rate': f"{compare_result.get('matching_rate', 0.0):.2%}",  # 在这里进行格式化
            'compare_cost_minute': f"{compare_result.get('compare_cost_minute', 0.0):.2f}",
            'compare_status': compare_result.get('compare_status', ''),
            'compare_report': compare_result.get('compare_report', '')
        }
        html_content = template.render(**formatted_result)
        return html_content
    except Exception as e:
        logger.error(f"生成HTML报告失败：{str(e)}")
        return f"<p>报告生成失败：{str(e)}</p>"

