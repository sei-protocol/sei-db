#!/bin/bash

echo "=== Memory Information ==="
free -h

echo ""
echo "=== Snapshot Sizes ==="
du -sh ~/.sei/data/committer.db/current/*/

echo ""
echo "=== Total Snapshot Size ==="
du -sh ~/.sei/data/committer.db/current/

echo ""
echo "=== Page Cache Usage ==="
# Check page cache for EVM tree
if [ -f ~/.sei/data/committer.db/current/evm/nodes ]; then
    echo "EVM nodes file:"
    ls -lh ~/.sei/data/committer.db/current/evm/nodes
fi

echo ""
echo "=== Memory Pressure ==="
vmstat 1 3

echo ""
echo "=== Swap Usage ==="
swapon --show


