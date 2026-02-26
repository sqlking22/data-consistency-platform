#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DataX修复引擎文件命名示例

演示新的文件命名规则：
1. 单批次：目标表名.json
2. 多批次：目标表名_1.json, 目标表名_2.json, 目标表名_3.json, ...
"""

def demo_filename_generation():
    """演示不同场景下的文件名生成"""

    print("=" * 80)
    print("DataX修复引擎 - 文件命名规则演示")
    print("=" * 80)
    print()

    # 场景1: 单批次（差异记录 ≤ 3000条）
    print("【场景1】单批次 - 差异记录100条")
    print("-" * 80)
    print("目标表名: users")
    print("差异记录数: 100条")
    print("生成文件: users.json")
    print("说明: 差异记录少于3000条，生成单个文件")
    print()

    # 场景2: 刚好3000条（单批次）
    print("【场景2】单批次 - 差异记录3000条")
    print("-" * 80)
    print("目标表名: orders")
    print("差异记录数: 3000条")
    print("生成文件: orders.json")
    print("说明: 差异记录等于3000条，生成单个文件")
    print()

    # 场景3: 2个批次（3000 < 差异记录 ≤ 6000）
    print("【场景3】多批次 - 差异记录4500条")
    print("-" * 80)
    print("目标表名: transactions")
    print("差异记录数: 4500条")
    print("生成文件:")
    print("  - transactions_1.json  (记录1-3000)")
    print("  - transactions_2.json  (记录3001-4500)")
    print("说明: 差异记录超过3000条，自动分批，每批最多3000条")
    print()

    # 场景4: 3个批次（6000 < 差异记录 ≤ 9000）
    print("【场景4】多批次 - 差异记录8000条")
    print("-" * 80)
    print("目标表名: payment_records")
    print("差异记录数: 8000条")
    print("生成文件:")
    print("  - payment_records_1.json  (记录1-3000)")
    print("  - payment_records_2.json  (记录3001-6000)")
    print("  - payment_records_3.json  (记录6001-8000)")
    print("说明: 根据记录数自动计算批次，8000条需要3个批次")
    print()

    # 场景5: 大数据量（10个批次）
    print("【场景5】大数据量 - 差异记录30000条")
    print("-" * 80)
    print("目标表名: log_data")
    print("差异记录数: 30000条")
    print("生成文件:")
    print("  - log_data_1.json   (记录1-3000)")
    print("  - log_data_2.json   (记录3001-6000)")
    print("  - log_data_3.json   (记录6001-9000)")
    print("  - log_data_4.json   (记录9001-12000)")
    print("  - log_data_5.json   (记录12001-15000)")
    print("  - log_data_6.json   (记录15001-18000)")
    print("  - log_data_7.json   (记录18001-21000)")
    print("  - log_data_8.json   (记录21001-24000)")
    print("  - log_data_9.json   (记录24001-27000)")
    print("  - log_data_10.json  (记录27001-30000)")
    print("说明: 大数据量自动分批，每个文件处理3000条记录")
    print()

    # 场景6: 联合主键表
    print("【场景6】联合主键表 - 差异记录5000条")
    print("-" * 80)
    print("目标表名: user_order_detail")
    print("主键: (user_id, order_id)")
    print("差异记录数: 5000条")
    print("生成文件:")
    print("  - user_order_detail_1.json  (记录1-3000)")
    print("  - user_order_detail_2.json  (记录3001-5000)")
    print("说明: 联合主键同样支持批量处理")
    print()

    print("=" * 80)
    print("关键特性:")
    print("=" * 80)
    print("1. 文件名直观：直接使用目标表名，便于识别")
    print("2. 批量处理：自动分批，每批最多3000条记录")
    print("3. 编号清晰：多批次时使用_1, _2, _3等后缀")
    print("4. WHERE条件优化：使用IN语法提高查询效率")
    print("5. 字段完整：同步源端和目标端所有相交字段")
    print()


def demo_file_path_examples():
    """演示实际文件路径示例"""

    print("\n" + "=" * 80)
    print("实际文件路径示例（假设DATAX_HOME=/data/datax-3.0）")
    print("=" * 80)
    print()

    examples = [
        ("users.json", "/data/datax-3.0/job/20260226/users.json"),
        ("orders.json", "/data/datax-3.0/job/20260226/orders.json"),
        ("transactions_1.json", "/data/datax-3.0/job/20260226/transactions_1.json"),
        ("transactions_2.json", "/data/datax-3.0/job/20260226/transactions_2.json"),
        ("log_data_10.json", "/data/datax-3.0/job/20260226/log_data_10.json"),
    ]

    for filename, full_path in examples:
        print(f"{filename:30} -> {full_path}")

    print()


if __name__ == '__main__':
    demo_filename_generation()
    demo_file_path_examples()
