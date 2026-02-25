# Data Consistency Platform

跨库数据一致性校验与修复平台，支持多种数据库之间的数据比对、差异发现和自动修复。

## 功能特性

- **多数据库支持**：MySQL、Oracle、PostgreSQL、SQLServer
- **智能比对引擎**：根据数据量自动选择 Pandas（<50万）或 Spark（>50万）
- **增量/全量比对**：支持基于时间字段的增量比对，提升效率
- **自动修复**：通过 DataX 工具自动同步差异数据
- **告警通知**：企业微信机器人发送比对和修复告警
- **监控集成**：Prometheus 指标收集
- **调度集成**：支持 DolphinScheduler 任务调度
- **多任务并发**：线程池并发处理多个比对任务
- **安全加密**：支持 AES 加密数据库密码
- **HTML 报告**：生成可视化比对报告

## 技术栈

| 类别 | 技术 |
|------|------|
| 编程语言 | Python 3.11 |
| 数据处理 | Pandas 2.1.4 / PySpark 3.5.0 |
| 数据比对 | datacompy 0.11.0 |
| ORM框架 | SQLAlchemy 2.0.23 |
| 数据库驱动 | PyMySQL / cx-Oracle / psycopg2 / pyodbc |
| 任务调度 | APScheduler 3.10.4 |
| 命令行工具 | Click 8.1.7 |
| 模板引擎 | Jinja2 3.1.2 |
| 加密工具 | pycryptodome 3.19.0 |

## 环境要求

- Python 3.11+
- Java 8+（运行 DataX 需要）
- Spark 3.x（大数据量比对需要）

## 安装部署

### 1. 克隆项目

```bash
git clone <repository_url>
cd data-consistency-platform
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 安装 DataX（用于数据修复）

```bash
# 下载 DataX
wget https://datax-opensource.oss-cn-hangzhou.aliyuncs.com/202210/datax.tar.gz
tar -zxvf datax.tar.gz -C /data/

# 验证安装
python /data/datax/bin/datax.py /data/datax/job/job.json
```

### 4. 配置数据库连接

编辑 `config/settings.py`，配置任务管理数据库：

```python
TASK_DB_CONFIG = {
    "db_type": "mysql",
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "123456",
    "database": "test"
}
```

## 配置说明

### 配置优先级

系统支持多层级配置，优先级从高到低：

1. **命令行参数** - 最高优先级
2. **settings.py** - 系统默认配置
3. **JSON 配置文件** - 任务级配置
4. **数据库配置** - 从 TASK_CONFIG_TABLE 读取

### 系统配置 (config/settings.py)

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| LOG_LEVEL | 日志级别 | INFO |
| WX_WORK_ROBOT | 企业微信机器人 Key | - |
| WX_ALERT_THRESHOLD | 差异率告警阈值 | 0.00 |
| DATAX_HOME | DataX 安装目录 | /data/datax-3.0 |
| BATCH_SIZE | 批量处理大小 | 1000 |
| MAX_RECORDS_THRESHOLD | Pandas引擎数据量阈值 | 300001 |
| ENGINE_STRATEGY | 比对引擎策略 | auto |
| ENABLE_REPAIR | 是否启用修复 | True |
| IS_INCREMENTAL | 是否增量比对 | True |
| INCREMENTAL_DAYS | 增量天数 | 1 |

### 任务配置 (config/config.json)

```json
{
  "id": 1,
  "src_db_type": "mysql",
  "src_host": "localhost",
  "src_port": 3306,
  "src_username": "root",
  "src_password": "123456",
  "src_db_name": "test",
  "src_table_name": "source_table",
  "tgt_db_type": "mysql",
  "tgt_host": "localhost",
  "tgt_port": 3306,
  "tgt_username": "root",
  "tgt_password": "123456",
  "tgt_db_name": "test",
  "tgt_table_name": "target_table",
  "update_time_str": "update_time",
  "sensitive_str": ""
}
```

### 数据库任务表结构

```sql
CREATE TABLE task_config_info (
    id INT PRIMARY KEY AUTO_INCREMENT,
    src_db_id INT,
    tgt_db_id INT,
    src_db_type VARCHAR(50),
    src_host VARCHAR(100),
    src_port INT,
    src_username VARCHAR(50),
    src_password VARCHAR(200),
    src_db_name VARCHAR(100),
    src_table_name VARCHAR(100),
    tgt_db_type VARCHAR(50),
    tgt_host VARCHAR(100),
    tgt_port INT,
    tgt_username VARCHAR(50),
    tgt_password VARCHAR(200),
    tgt_db_name VARCHAR(100),
    tgt_table_name VARCHAR(100),
    update_time_str VARCHAR(50),
    sensitive_str VARCHAR(200),
    status TINYINT DEFAULT 1
);

CREATE TABLE task_result_log (
    id INT PRIMARY KEY AUTO_INCREMENT,
    task_id INT,
    start_time DATETIME,
    end_time DATETIME,
    src_count INT,
    tgt_count INT,
    diff_count INT,
    diff_rate DECIMAL(10,4),
    status VARCHAR(20),
    error_msg TEXT
);
```

## 使用方法

### 命令行参数

```bash
python main.py [OPTIONS]
```

| 参数 | 说明 | 默认值 |
|------|------|--------|
| --table_id | 任务ID（从数据库读取配置） | - |
| --config_file | JSON配置文件路径 | config/config.json |
| --incremental | 是否增量比对 | False |
| --incremental_days | 增量天数 | 1 |
| --concurrency | 并发数 | 5 |
| --enable_repair | 是否启用修复 | True |

### 使用示例

#### 1. 使用 JSON 配置文件执行单任务

```bash
python main.py --config_file config/config.json
```

#### 2. 使用数据库配置执行单任务

```bash
python main.py --table_id 1
```

#### 3. 增量比对（最近3天数据）

```bash
python main.py --table_id 1 --incremental --incremental_days 3
```

#### 4. 批量执行多任务（并发数3）

```bash
python main.py --concurrency 3
```

#### 5. 只比对不修复

```bash
python main.py --table_id 1 --enable_repair false
```

### DolphinScheduler 集成

在 DolphinScheduler 中配置 Shell 任务：

```bash
#!/bin/bash
cd /path/to/data-consistency-platform
/usr/bin/python311 main.py --table_id ${taskId} --incremental --incremental_days 1
```

## 项目结构

```
data-consistency-platform/
├── config/                          # 配置目录
│   ├── config.json                  # JSON格式的任务配置文件
│   └── settings.py                  # 系统参数配置
├── core/                            # 核心模块
│   ├── compare_engine/              # 比对引擎
│   │   ├── base_engine.py           # 比对引擎基类
│   │   ├── pandas_engine.py         # Pandas比对引擎（小数据量）
│   │   └── spark_engine.py          # Spark比对引擎（大数据量）
│   ├── db_adapter/                  # 数据库适配器
│   │   ├── base_adapter.py          # 适配器基类
│   │   ├── mysql_adapter.py         # MySQL适配器
│   │   ├── oracle_adapter.py        # Oracle适配器
│   │   ├── postgres_adapter.py      # PostgreSQL适配器
│   │   └── sqlserver_adapter.py     # SQLServer适配器
│   ├── repair_engine/               # 修复引擎
│   │   ├── base_repair.py           # 修复引擎基类
│   │   └── datax_repair.py          # DataX修复实现
│   ├── config_manager.py            # 配置管理中心
│   ├── monitor.py                   # 监控指标
│   └── notification.py              # 企业微信通知
├── utils/                           # 工具类
│   ├── crypto_utils.py              # AES加解密工具
│   ├── data_type_utils.py           # 数据类型转换
│   ├── db_utils.py                  # 数据库工具
│   ├── logger.py                    # 日志系统
│   ├── report_utils.py              # 报告生成工具
│   └── retry_utils.py               # 重试工具
├── scheduler/                       # 调度集成
│   └── dolphin_scheduler.py         # DolphinScheduler集成
├── tests/                           # 测试用例
│   ├── conftest.py                  # pytest配置和夹具
│   ├── test_compare_engine.py       # 比对引擎测试
│   ├── test_db_adapter.py           # 数据库适配器测试
│   ├── test_repair_engine.py        # 修复引擎测试
│   └── test_utils.py                # 工具类测试
├── logs/                            # 日志目录
├── main.py                          # 主程序入口
├── requirements.txt                 # Python依赖清单
└── README.md                        # 项目说明文档
```

## 核心模块说明

### 比对引擎 (compare_engine)

| 引擎 | 适用场景 | 数据量 | 依赖 |
|------|----------|--------|------|
| PandasEngine | 小数据量 | <50万 | pandas, datacompy |
| SparkEngine | 大数据量 | >50万 | pyspark |

比对流程：
1. 从源端和目标端抽取数据
2. 数据类型标准化处理
3. 按主键排序后逐行比对
4. 生成差异报告

### 数据库适配器 (db_adapter)

统一的数据库操作接口，支持：
- 连接池管理
- 查询执行
- 批量操作
- 事务支持

### 修复引擎 (repair_engine)

基于 DataX 的数据修复：
1. 生成差异数据的主键列表
2. 动态生成 DataX Job 配置
3. 执行 DataX 同步任务
4. 验证修复结果

### 通知模块 (notification)

企业微信机器人告警：
- 比对完成通知（含差异数量、差异率）
- 修复完成通知
- 异常告警通知

## 开发指南

### 运行测试

```bash
# 运行所有测试
pytest tests/

# 运行指定测试文件
pytest tests/test_compare_engine.py -v

# 生成覆盖率报告
pytest tests/ --cov=core --cov=utils --cov-report=html
```

### 添加新的数据库适配器

1. 在 `core/db_adapter/` 下创建新适配器文件
2. 继承 `BaseAdapter` 基类
3. 实现必要的抽象方法
4. 在适配器工厂中注册

```python
from core.db_adapter.base_adapter import BaseAdapter

class NewDBAdapter(BaseAdapter):
    def __init__(self, db_config):
        super().__init__(db_config)

    def connect(self):
        # 实现连接逻辑
        pass

    def execute_query(self, sql, params=None):
        # 实现查询逻辑
        pass
```

### 密码加密

支持 AES 加密存储数据库密码：

```python
from utils.crypto_utils import CryptoUtils

crypto = CryptoUtils()
encrypted = crypto.encrypt("your_password")
decrypted = crypto.decrypt(encrypted)
```

## 常见问题

### 1. Oracle 连接问题

确保已安装 Oracle Instant Client，并配置环境变量：

```bash
export LD_LIBRARY_PATH=/path/to/instantclient:$LD_LIBRARY_PATH
```

### 2. Spark 内存不足

调整 Spark 配置：

```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
```

### 3. DataX 中文乱码

确保 DataX Job 配置中指定编码：

```json
{
  "job": {
    "setting": {
      "speed": {"channel": 3}
    },
    "content": [{
      "reader": {...},
      "writer": {
        "parameter": {
          "encoding": "UTF-8"
        }
      }
    }]
  }
}
```

## License

MIT License
