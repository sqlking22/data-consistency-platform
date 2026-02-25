# 测试套件说明

## 测试结构

```
tests/
├── conftest.py                  # pytest配置和公共夹具
├── test_config_manager.py       # 配置管理器测试
├── test_db_adapter.py           # 数据库适配器测试
├── test_compare_engine.py       # 比对引擎测试
├── test_repair_engine.py        # 修复引擎测试
├── test_notification.py         # 通知模块测试
├── test_main.py                 # 主流程测试
└── test_utils.py                # 工具类测试
```

## 测试覆盖范围

### 1. 配置管理器测试 (test_config_manager.py)
- ✅ 配置加载优先级 (命令行 > settings.py > JSON > 数据库)
- ✅ JSON配置文件验证
- ✅ 数据库配置加载
- ✅ 密码解密
- ✅ 多任务模式
- ✅ 边界条件和异常处理

### 2. 数据库适配器测试 (test_db_adapter.py)
- ✅ MySQL/Oracle/PostgreSQL/SQLServer适配器
- ✅ 连接管理
- ✅ 查询和执行操作
- ✅ 元数据获取
- ✅ 重试机制
- ✅ 事务处理
- ✅ 特殊字符和Unicode支持

### 3. 比对引擎测试 (test_compare_engine.py)
- ✅ Pandas引擎 (小数据量)
- ✅ Spark引擎 (大数据量)
- ✅ 增量/全量比对
- ✅ 主键和字段获取
- ✅ 敏感字段过滤
- ✅ 数据差异检测
- ✅ 引擎自动选择

### 4. 修复引擎测试 (test_repair_engine.py)
- ✅ DataX作业生成
- ✅ Reader/Writer配置
- ✅ JDBC URL生成
- ✅ 修复阈值检查
- ✅ 修复执行和验证
- ✅ 异常处理

### 5. 通知模块测试 (test_notification.py)
- ✅ 企业微信通知
- ✅ 告警阈值判断
- ✅ 比对和修复告警
- ✅ Unicode和特殊字符
- ✅ 网络异常处理

### 6. 主流程测试 (test_main.py)
- ✅ 单任务处理
- ✅ 多任务并发
- ✅ 配置加载
- ✅ 日志写入
- ✅ 错误处理
- ✅ 耗时统计

### 7. 工具类测试 (test_utils.py)
- ✅ 数据库工具
- ✅ 加密解密
- ✅ 数据类型转换
- ✅ 重试机制
- ✅ 报告生成

## 运行测试

### 安装测试依赖

```bash
pip install pytest pytest-mock pytest-cov
```

### 运行所有测试

```bash
# 方式1: 使用测试脚本
python run_tests.py

# 方式2: 直接使用pytest
pytest tests/ -v --cov=core --cov=utils --cov-report=html --cov-fail-under=95
```

### 运行特定测试

```bash
# 运行单个测试文件
pytest tests/test_config_manager.py -v

# 运行特定测试类
pytest tests/test_db_adapter.py::TestMySQLAdapter -v

# 运行特定测试方法
pytest tests/test_compare_engine.py::TestPandasCompareEngine::test_compare_with_differences -v
```

### 快速测试(跳过慢速测试)

```bash
pytest tests/ -v -m "not slow"
```

### 生成覆盖率报告

```bash
pytest tests/ --cov=core --cov=utils --cov-report=html --cov-report=xml
```

查看HTML报告: `htmlcov/index.html`

## 测试覆盖率目标

| 模块 | 目标覆盖率 | 说明 |
|------|-----------|------|
| core/config_manager.py | ≥95% | 配置管理核心 |
| core/db_adapter/ | ≥95% | 数据库适配器 |
| core/compare_engine/ | ≥95% | 比对引擎 |
| core/repair_engine/ | ≥95% | 修复引擎 |
| core/notification.py | ≥95% | 通知模块 |
| utils/ | ≥95% | 工具类 |
| **总体** | **≥95%** | **核心逻辑** |

## 常见问题

### 1. 导入错误

**问题**: `ModuleNotFoundError: No module named 'core'`

**解决**:
```bash
# 确保在项目根目录运行测试
cd /path/to/data-consistency-platform
pytest tests/
```

### 2. 数据库连接错误

**问题**: 测试需要数据库连接

**解决**: 所有数据库测试都使用Mock,不需要真实数据库连接

### 3. 覆盖率不达标

**问题**: `FAIL Required test coverage of 95% not reached. Coverage: 85%`

**解决**:
1. 查看HTML报告找出未覆盖的代码
2. 添加更多测试用例覆盖分支逻辑
3. 特别关注异常处理分支

### 4. 测试超时

**问题**: 某些测试运行很慢

**解决**:
```bash
# 跳过慢速测试
pytest tests/ -m "not slow"
```

### 5. Mock相关问题

**问题**: Mock不生效或AssertionError

**解决**:
```python
# 确保Mock的路径正确
with patch('module.submodule.function') as mock_func:
    # 而不是
    # with patch('other_module.function') as mock_func:
```

## 持续集成

### GitHub Actions示例

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v2

    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: 3.11

    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install pytest pytest-mock pytest-cov

    - name: Run tests
      run: |
        pytest tests/ --cov=core --cov=utils --cov-report=xml --cov-fail-under=95

    - name: Upload coverage
      uses: codecov/codecov-action@v2
```

## 测试最佳实践

1. **测试独立性**: 每个测试应该独立,不依赖其他测试
2. **使用夹具**: 通过conftest.py共享测试数据
3. **Mock外部依赖**: 数据库、网络等使用Mock
4. **覆盖边界条件**: 不仅是正常流程,还要测试异常情况
5. **清晰的测试名**: 测试名应该清楚说明测试内容
6. **适当的断言**: 使用具体的断言,避免过于宽泛
7. **测试文档**: 为复杂的测试添加注释

## 测试指标

```bash
# 查看测试统计
pytest tests/ --co -q

# 查看最慢的测试
pytest tests/ --durations=10

# 详细输出
pytest tests/ -vv --tb=long
```
