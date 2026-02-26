# 时间字段智能过滤逻辑说明

## 背景

在增量比对场景下，基于时间范围筛选数据时，会出现以下复杂场景：

### 场景1：源端更新，目标端未更新
- **源端记录**：主键ID=1，更新时间=今天 10:00
- **目标端记录**：主键ID=1，更新时间=昨天 20:00
- **比对范围**：今天 00:00 到今天 23:59
- **比对结果**：`mismatch`（两端都有，但时间不同）
- **修复决策**：源端时间 > 目标端时间 → **需要修复** ✅

### 场景2：目标端更新，源端未更新（危险场景）
- **源端记录**：主键ID=2，更新时间=昨天 20:00
- **目标端记录**：主键ID=2，更新时间=今天 10:00
- **比对范围**：今天 00:00 到今天 23:59
- **比对结果**：`tgt_only`（目标端独有）
- **修复决策**：目标端数据更新，源端落后 → **不应该修复** ⚠️

### 场景3：源端独有记录（可能的时间差）
- **源端记录**：主键ID=3，更新时间=昨天 20:00
- **目标端记录**：主键ID=3，更新时间=今天 10:00
- **比对范围**：昨天 00:00 到昨天 23:59
- **比对结果**：`src_only`（源端独有）
- **修复决策**：
  - 如果直接修复：会用昨天20:00的旧数据覆盖今天10:00的新数据 ��
  - **智能判断**：查询目标端实际记录，发现目标端时间更新 → **跳过修复** ✅

### 场景4：源端独有记录（目标端真的没有）
- **源端记录**：主键ID=4，更新时间=今天 10:00
- **目标端记录**：不存在
- **比对范围**：今天 00:00 到今天 23:59
- **比对结果**：`src_only`（源端独有）
- **修复决策**：查询目标端确实不存在 → **需要修复（插入）** ✅

## 解决方案

### 核心逻辑

对于所有差异数据（`mismatch` 和 `src_only`），都执行时间字段验证：

1. **mismatch 记录**：
   - 比对时已保存源端和目标端完整记录
   - 直接比较时间字段：`源端时间 - 目标端时间 > TIME_TOLERANCE`
   - 满足条件才加入修复列表

2. **src_only 记录**：
   - 比对时只保存源端记录（目标端在比对时间范围内不存在）
   - **关键步骤**：实时查询目标端该主键是否存在
   - 如果目标端存在：
     - 比较时间字段：`源端时间 - 目标端时间 > TIME_TOLERANCE`
     - 源端更新才修复，否则跳过（避免旧数据覆盖新数据）
   - 如果目标端不存在：
     - 需要修复（插入新记录）

### 配置项

```json
{
  "update_time_str": "update_time",      // 时间字段名称（必需）
  "enable_time_filter": true,            // 是否启用时间过滤（可选，默认true）
  "TIME_TOLERANCE": 0                    // 时间容差（秒），可选，默认0
}
```

### 代码实现

#### 1. 比对引擎增强（core/compare_engine/pandas_engine.py）

```python
# 存储完整的差异数据，包含源端和目标端记录
diff_records = {
    'mismatch': [],      # 主键信息（向后兼容）
    'mismatch_full': [], # 完整记录：源端+目标端
    'src_only': [],      # 源端独有（完整记录）
    'tgt_only': []       # 目标端独有
}
```

#### 2. 修复引擎智能过滤（core/repair_engine/datax_repair.py）

```python
def _build_where_clauses_batch(self):
    # 处理 mismatch 记录
    for record_data in mismatch_full:
        src_time = record_data['src_record'].get(update_time_col)
        tgt_time = record_data['tgt_record'].get(update_time_col)

        # 源端时间 > 目标端时间 + 容差，才修复
        if (src_time - tgt_time).total_seconds() > TIME_TOLERANCE:
            all_diff_keys.append(pk)

    # 处理 src_only 记录
    for record in src_only_records:
        src_time = record.get(update_time_col)

        # 查询目标端实际记录
        tgt_record = self._query_target_record(pk_dict, pk_columns, [update_time_col])

        if tgt_record is None:
            # 目标端不存在，需要插入
            all_diff_keys.append(pk_dict)
        else:
            # 目标端存在，比较时间
            tgt_time = tgt_record.get(update_time_col)
            if (src_time - tgt_time).total_seconds() > TIME_TOLERANCE:
                all_diff_keys.append(pk_dict)
            else:
                logger.info(f"跳过修复：源端时间{src_time} <= 目标端时间{tgt_time}")
```

## 优势

1. **避免错误修复**：不会用源端的旧数据覆盖目标端的新数据
2. **精准修复**：只修复真正需要修复的记录（源端更新的数据）
3. **详细日志**：记录每条记录的修复决策原因，便于审计
4. **性能优化**：减少不必要的修复操作

## 日志示例

```
[INFO] 启用时间字段过滤：update_time，时间容差：0秒
[INFO] 记录{'id': 1}需要修复：源端时间2026-02-26 10:00:00 > 目标端时间2026-02-25 20:00:00（差50400秒）
[INFO] 记录{'id': 2}无需修复：源端时间2026-02-25 20:00:00 <= 目标端时间2026-02-26 10:00:00（差-50400秒）
[INFO] 源端独有记录{'id': 3}无需修复：源端时间2026-02-25 20:00:00 <= 目标端时间2026-02-26 10:00:00（差-50400秒）- 避免用旧数据覆盖新数据
[INFO] 源端独有记录{'id': 4}：目标端不存在，需要插入
[INFO] 源端独有记录共4条：需要修复2条，跳过2条（避免旧数据覆盖新数据）
```
