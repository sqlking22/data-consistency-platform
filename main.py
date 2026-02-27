#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 12:45
# @Author  : hejun
import logging
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from core.config_manager import ConfigManager
from core.compare_engine.base_engine import get_compare_engine
from core.repair_engine.datax_repair import DataXRepairEngine
from core.notification import WeChatNotification
from config.settings import MAX_THREAD_COUNT, TASK_DB_CONFIG, TASK_LOG_TABLE, TASK_CONFIG_TABLE, LOG_LEVEL
from utils.db_utils import write_task_log
from utils.log_utils import setup_logging

# 配置日志
setup_logging(log_level=LOG_LEVEL)

# ���制第三方库的详细日志
logger = logging.getLogger(__name__)


def process_single_table(config: dict):
    """处理单表比对和修复"""
    notification = WeChatNotification()

    try:
        # 1. 执行比对
        logger.info(f"开始比对表：{config['src_table_name']}")
        compare_engine = get_compare_engine(config)
        compare_result = compare_engine.run()

        # 2. 发送比对告警（如有）
        # notification.send_compare_alert(config, compare_result)

        # 3. 执行修复（如需）
        repair_result = {}

        if config.get('enable_repair', False):
            logger.info(f"开始修复表：{config['src_table_name']}")
            repair_engine = DataXRepairEngine(config, compare_result)
            repair_result = repair_engine.repair()

            # 发送修复告警（如有）
            # notification.send_repair_alert(config, repair_result)

        # 4. 合并结果
        total_result = {**config, **compare_result, **repair_result}

        # 5. 计算总耗时
        compare_cost = compare_result.get('compare_cost_minute', 0)
        repair_cost = repair_result.get('repair_cost_minute', 0)
        total_result['compare_total_cost_minute'] = round(compare_cost + repair_cost, 6)

        # 6. 写入日志表
        total_result['compare_time'] = datetime.now()
        total_result['config_id'] = config.get('id')
        total_result['src_db_id'] = config.get('src_db_id')

        # 6. 准备写入数据库的字段（只包含表中存在的字段）
        valid_fields = {
            'table_id': config.get('table_id'),
            'compare_time': datetime.now(),
            'compare_status': compare_result.get('compare_status'),
            'compare_msg': compare_result.get('compare_msg', ''),
            'src_db_id': str(config.get('src_db_id', '')),
            'src_db_name': config.get('src_db_name'),
            'src_table_name': config.get('src_table_name'),
            'tgt_db_name': config.get('tgt_db_name'),
            'tgt_table_name': config.get('tgt_table_name'),
            'check_range': compare_result.get('check_range'),
            'check_column': compare_result.get('check_column'),
            'src_cnt': compare_result.get('src_cnt', 0),
            'tgt_cnt': compare_result.get('tgt_cnt', 0),
            'compare_start_time': compare_result.get('compare_start_time'),
            'compare_end_time': compare_result.get('compare_end_time'),
            'compare_cost_minute': compare_result.get('compare_cost_minute', 0.0),
            'diff_cnt': compare_result.get('diff_cnt', 0),
            'compare_report': compare_result.get('compare_report'),
            'html_report': compare_result.get('html_report'),
            'repair_status': repair_result.get('repair_status'),
            'repair_cnt': repair_result.get('repair_cnt', 0),
            'repair_start_time': repair_result.get('repair_start_time'),
            'repair_end_time': repair_result.get('repair_end_time'),
            'repair_cost_minute': repair_result.get('repair_cost_minute', 0.0),
            'repair_job_file': repair_result.get('repair_job_file'),
            'compare_total_cost_minute': total_result.get('compare_total_cost_minute', 0.0),
            'is_delete': 0,
            'create_time': datetime.now(),
            'update_time': datetime.now(),
            'repair_msg': repair_result.get('repair_msg', '')
        }

        # 移除值为 None 的字段，避免插入 NULL 值
        filtered_result = {k: v for k, v in valid_fields.items() if v is not None}

        write_task_log(TASK_DB_CONFIG, TASK_LOG_TABLE, filtered_result)

        logger.info(f"表{config['src_table_name']}处理完成，差异记录数：{compare_result.get('diff_cnt', 0)}")

        return total_result

    except Exception as e:
        logger.error(f"处理表{config.get('src_table_name')}失败：{str(e)}")
        # 写入失败日志
        fail_result = {
            'table_id': config.get('table_id'),
            'compare_time': datetime.now(),
            'compare_status': 'fail',
            'compare_msg': str(e),
            'src_db_id': str(config.get('src_db_id', '')),
            'src_db_name': config.get('src_db_name'),
            'src_table_name': config.get('src_table_name'),
            'tgt_db_name': config.get('tgt_db_name'),
            'tgt_table_name': config.get('tgt_table_name'),
            'compare_start_time': datetime.now(),
            'compare_end_time': datetime.now(),
            'compare_cost_minute': 0,
            'diff_cnt': 0,
            'repair_status': 'fail',
            'repair_msg': str(e),
            'is_delete': 0,
            'create_time': datetime.now(),
            'update_time': datetime.now()
        }

        # 过滤掉 None 值
        filtered_fail_result = {k: v for k, v in fail_result.items() if v is not None}

        write_task_log(TASK_DB_CONFIG, TASK_LOG_TABLE, filtered_fail_result)
        raise


def main():
    """主函数"""
    start_time = time.time()

    # 1. 加载配置
    config_manager = ConfigManager()
    config_result = config_manager.load_all_configs()

    # 2. 判断配置类型并处理
    if isinstance(config_result, dict) and 'task_configs' in config_result:
        # 多任务模式
        global_config = config_result['global_config']
        task_configs = config_result['task_configs']

        # 多线程处理
        concurrency = global_config.get('concurrency', MAX_THREAD_COUNT)
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []
            for task in task_configs:
                # 合并全局配置和任务配置
                merged_config = {**global_config, **task}
                futures.append(executor.submit(process_single_table, merged_config))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"任务执行失败：{str(e)}")
    else:
        # 单任务模式
        config = config_result

        # 2. 处理单表或多表
        table_id = config.get('table_id')

        if table_id:
            # 从数据库查询指定table_id的配置并执行
            from core.db_adapter.base_adapter import get_db_adapter
            adapter = get_db_adapter(TASK_DB_CONFIG)
            try:
                sql = f"SELECT * FROM {TASK_CONFIG_TABLE} WHERE id = %s AND is_delete = 0"
                result = adapter.query(sql, (table_id,))
                if result:
                    task_config = dict(result[0])
                    # 合并全局配置和任务配置
                    merged_config = {**config, **task_config}
                    process_single_table(merged_config)
                else:
                    raise ValueError(f"找不到ID为{table_id}的任务配置")
            finally:
                adapter.close()
        else:
            # 执行JSON配置的任务
            process_single_table(config)

    logger.info(f"所有任务执行完成，总耗时：{round((time.time() - start_time) / 60, 2)}分钟")


if __name__ == "__main__":
    main()