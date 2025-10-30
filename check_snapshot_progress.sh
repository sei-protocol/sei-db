#!/bin/bash
# 快速检查 snapshot 写入进度

echo "=== Snapshot 进度检查 ==="
echo ""

# 查找临时 snapshot 目录
SNAPSHOT_TMP=$(find ~/.sei/data/committer.db/ -name "snapshot-*-tmp" -type d 2>/dev/null | head -1)

if [ -z "$SNAPSHOT_TMP" ]; then
    echo "❌ 没有找到临时 snapshot 目录"
    exit 1
fi

echo "📁 临时 Snapshot 目录: $SNAPSHOT_TMP"
echo ""

# 检查 EVM 目录
if [ -d "$SNAPSHOT_TMP/evm" ]; then
    echo "📊 EVM 树状态:"
    EVM_SIZE=$(du -sh "$SNAPSHOT_TMP/evm" | cut -f1)
    echo "  总大小: $EVM_SIZE"
    
    if [ -f "$SNAPSHOT_TMP/evm/nodes" ]; then
        NODES_SIZE=$(du -sh "$SNAPSHOT_TMP/evm/nodes" | cut -f1)
        echo "  nodes 文件: $NODES_SIZE"
    fi
    
    if [ -f "$SNAPSHOT_TMP/evm/leaves" ]; then
        LEAVES_SIZE=$(du -sh "$SNAPSHOT_TMP/evm/leaves" | cut -f1)
        echo "  leaves 文件: $LEAVES_SIZE"
    fi
    
    if [ -f "$SNAPSHOT_TMP/evm/kvs" ]; then
        KVS_SIZE=$(du -sh "$SNAPSHOT_TMP/evm/kvs" | cut -f1)
        echo "  kvs 文件: $KVS_SIZE"
    fi
    echo ""
fi

# 检查所有树的完成状态
echo "📋 所有树的状态:"
for tree_dir in "$SNAPSHOT_TMP"/*; do
    if [ -d "$tree_dir" ]; then
        tree_name=$(basename "$tree_dir")
        tree_size=$(du -sh "$tree_dir" | cut -f1)
        
        # 检查是否有 metadata 文件（表示完成）
        if [ -f "$tree_dir/metadata" ]; then
            echo "  ✅ $tree_name ($tree_size)"
        else
            echo "  🔄 $tree_name ($tree_size) - 正在写入"
        fi
    fi
done
echo ""

# 检查最近的日志
echo "📝 最近的 SNAPSHOT WRITE 日志 (最后 5 条):"
journalctl -u seid --since "5 min ago" | grep "SNAPSHOT WRITE" | tail -5
echo ""

# 检查进程状态
echo "🔍 seid 进程状态:"
ps aux | grep seid | grep -v grep | awk '{print "  PID: " $2 ", CPU: " $3 "%, MEM: " $4 "%, TIME: " $10}'
echo ""

# 检查磁盘 I/O
echo "💾 磁盘 I/O 状态:"
iostat -x 1 2 | tail -n +4 | grep -v "^$" | tail -5


