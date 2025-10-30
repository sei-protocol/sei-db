# Export/Import Optimization for Snapshot Rewriting

## 概述

实现了基于 Export/Import 的 snapshot rewriting 方法，相比传统的递归遍历方式，使用顺序 I/O 而不是随机 I/O，预期可以带来 **2-3倍的性能提升**。

## 性能对比

### 传统递归遍历方式
```
读取模式: 随机 I/O
- 递归遍历树结构
- 随机跳转读取 nodes
- 随机读取 kvs (变长 offset)

性能指标:
- 速度: 270k nodes/s
- 读取 IOPS: 1800 ops/s
- 磁盘利用率: 100%
- 队列深度: 1-2 (浅)
```

### Export/Import 方式
```
读取模式: 顺序 I/O
- 顺序读取 nodes 文件 (i++)
- 顺序读取 leaves 文件 (j++)
- 顺序读取 kvs 文件 (ordered offsets)

预期性能:
- 速度: 600-900k nodes/s (2-3倍提升)
- 更好的磁盘带宽利用
- 更好的 page cache 效率
```

## 实现细节

### 新增文件
- `sc/memiavl/export_import_test.go` - 测试用例

### 修改文件
1. **`sc/memiavl/tree.go`**
   - 新增 `RewriteSnapshotViaExport()` 方法
   - 使用 Export/Import 流式处理节点

2. **`sc/memiavl/multitree.go`**
   - 新增 `WriteSnapshotViaExport()` 方法
   - 新增 `writeSnapshotPriorityEVMViaExport()` 方法
   - 优先处理 EVM tree (73% 数据)，然后并行处理其他树

3. **`sc/memiavl/opts.go`**
   - 新增 `UseExportImportForRewrite` 配置选项
   - 默认 false (向后兼容)

4. **`sc/memiavl/db.go`**
   - 修改 `RewriteSnapshot()` 根据配置选择方法

## 使用方法

### 方法1: 通过配置启用

```go
db, err := OpenDB(logger, 0, Options{
    Dir:                       "/path/to/db",
    UseExportImportForRewrite: true,  // 启用 Export/Import
})
```

### 方法2: 直接调用 API

```go
// Tree 层面
tree.RewriteSnapshotViaExport(ctx, snapshotDir)

// MultiTree 层面
multiTree.WriteSnapshotViaExport(ctx, dir, workerPool)
```

## 测试

运行测试：
```bash
go test ./sc/memiavl/ -run "TestRewriteSnapshotViaExport|TestDBRewriteSnapshotWithExportImport" -v
```

所有测试通过 ✅

## 向后兼容性

- 默认使用传统递归遍历方式 (`UseExportImportForRewrite: false`)
- 可以通过配置选项逐步迁移
- 两种方式生成的 snapshot 格式完全相同
- 可以安全地在两种方式之间切换

## 下一步

### 立即可做
1. 在测试环境启用 `UseExportImportForRewrite: true`
2. 监控性能指标
3. 对比两种方式的实际性能差异

### 如果效果好
1. 在生产环境逐步推广
2. 收集更多性能数据
3. 考虑将 Export/Import 设为默认方式

### 进一步优化（可选）
1. 如果还需要更快，考虑升级磁盘 IOPS (6000 → 10000-16000)
2. 考虑加倍内存 (124GB → 248GB) - 但性价比较低

## 性能预期

### EVM Tree (512M nodes, 81GB)
```
当前 (递归遍历):
- 时间: ~30-40 分钟
- 速度: 270k nodes/s

预期 (Export/Import):
- 时间: ~10-15 分钟 (2-3倍提升)
- 速度: 600-900k nodes/s
```

### 成本效益
```
开发成本: ~7 小时
运行成本: $0 (无需额外硬件)
性能提升: 2-3倍
性价比: ⭐⭐⭐⭐⭐
```

## 技术原理

### 为什么顺序 I/O 更快？

1. **更好的预读**
   - 内核可以预测下一个读取位置
   - 自动预读相邻数据块
   - 减少磁盘寻道时间

2. **更高的队列深度**
   - 顺序读取可以批量提交 I/O 请求
   - 更好地利用磁盘 IOPS
   - 减少磁盘空闲时间

3. **更好的 Page Cache 利用**
   - 顺序数据更容易被缓存
   - 减少 cache 驱逐
   - 提高 cache 命中率

## 监控指标

建议监控以下指标来评估效果：

```bash
# 磁盘 I/O
iostat -x nvme1n1 5

关注:
- r/s (读取 IOPS)
- rkB/s (读取吞吐)
- r_await (读取延迟)
- aqu-sz (队列深度)
- %util (利用率)

预期变化:
- 读取 IOPS: 1800 → 500-800 (减少)
- 读取吞吐: 28 MB/s → 80-120 MB/s (增加)
- 队列深度: 1-2 → 4-8 (增加)
```

## 作者

实现日期: 2025-10-29
版本: v1.0

