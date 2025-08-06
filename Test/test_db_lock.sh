#!/bin/bash

# 🚀 并发运行你的 Python 爬虫脚本，传入不同的 region_id 标识来源
# 你可以固定 region_id，例如：21 表示 “生活 - 日常”
REGION_ID=21

# 脚本路径（根据你保存的实际路径填写）
SCRIPT="/data2/luyz/project/CustomizedAnalysis/CustomizedAnalysis.12.20250726_YASbilibili/Code/1.spider_video_details_to_sqlite_with_lock.py"

# 并发数（你可以调整为 3~5 观察锁排队效果）
NUM_PROCESSES=3

echo "🧪 启动 $NUM_PROCESSES 个并发爬虫测试锁机制..."
echo "⏱️ 每个进程都会尝试访问同一个 SQLite 数据库..."

for i in $(seq 1 $NUM_PROCESSES)
do
    echo "🚀 启动进程 $i"
    python3 "$SCRIPT" "$REGION_ID" \
        --video_details_db /data2/luyz/project/CustomizedAnalysis/Log/test_video_details.db \
        --video_details_with_type_db /data2/luyz/project/CustomizedAnalysis/Log/test_video_details_with_type.db \
        --max_pages 100 \
        --interval 0.1 \
        --end_date "2025-08-01" \
        > /data2/luyz/project/CustomizedAnalysis/Log/log_$i.txt 2>&1 &   # 每个进程的输出写入不同 log 文件
done

wait
echo "✅ 所有进程已完成，可以检查 log_*.txt 日志文件查看锁竞争过程"
