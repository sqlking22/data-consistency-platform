#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试运行脚本
运行所有测试并生成覆盖率报告
"""
import sys
import subprocess
import os


def run_tests():
    """运行测试套件"""
    print("=" * 80)
    print("开始运行数据一致性平台测试套件")
    print("=" * 80)

    # 切换到项目根目录
    project_root = os.path.dirname(os.path.abspath(__file__))
    os.chdir(project_root)

    # 运行pytest
    cmd = [
        sys.executable, "-m", "pytest",
        "tests/",
        "-v",
        "--tb=short",
        "--cov=core",
        "--cov=utils",
        "--cov-report=term-missing",
        "--cov-report=html:htmlcov",
        "--cov-report=xml:coverage.xml",
        "--cov-fail-under=95",
        "-x",  # 第一个失败就停止
        "--durations=10"  # 显示最慢的10个测试
    ]

    print(f"\n执行命令: {' '.join(cmd)}\n")

    result = subprocess.run(cmd)

    if result.returncode == 0:
        print("\n" + "=" * 80)
        print("✅ 所有测试通过!")
        print("=" * 80)
        print("\n📊 覆盖率报告:")
        print(f"  - HTML报告: {os.path.join(project_root, 'htmlcov', 'index.html')}")
        print(f"  - XML报告: {os.path.join(project_root, 'coverage.xml')}")
        print("\n" + "=" * 80)
    else:
        print("\n" + "=" * 80)
        print("❌ 测试失败!")
        print("=" * 80)
        sys.exit(1)


def run_quick_tests():
    """快速测试(跳过慢速测试)"""
    print("运行快速测试...")
    cmd = [
        sys.executable, "-m", "pytest",
        "tests/",
        "-v",
        "-m", "not slow",
        "--tb=short"
    ]
    subprocess.run(cmd)


def run_specific_test(test_file):
    """运行特定测试文件"""
    print(f"运行测试: {test_file}")
    cmd = [
        sys.executable, "-m", "pytest",
        test_file,
        "-v",
        "--tb=short"
    ]
    subprocess.run(cmd)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='测试运行工具')
    parser.add_argument('--quick', action='store_true', help='快速测试(跳过慢速测试)')
    parser.add_argument('--file', type=str, help='运行特定测试文件')
    parser.add_argument('--no-cov', action='store_true', help='不生成覆盖率报告')

    args = parser.parse_args()

    if args.file:
        run_specific_test(args.file)
    elif args.quick:
        run_quick_tests()
    else:
        run_tests()
