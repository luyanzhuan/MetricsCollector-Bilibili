#!/bin/bash

# 获取当前脚本所在的目录（支持软链接和相对路径）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 构建配置文件路径：脚本同级目录的 Config/Example/201/config.conf
# 这里假设配置文件位于脚本目录下的，你需要根据实际情况调整路径或者参数信息
CONFIG_FILE="${SCRIPT_DIR}/Config/Example/201/config.conf"

# 检查配置文件是否存在
if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ 配置文件不存在: $CONFIG_FILE"
    exit 1
fi

# 加载配置
source "$CONFIG_FILE"

# 打印加载的参数（可选）
echo "✅ 已加载配置文件: $CONFIG_FILE"
echo "项目目录：$project_dir"
echo "结果目录：$result_dir"
echo "日志目录：$log_dir"
echo "分区ID：$region_id"
echo "最大页数：$max_pages"
echo "间隔时间：$interval 秒"

# 锁文件路径
lockfile="/tmp/metrics_collector_$region_id.lock"

# 尝试获取非阻塞锁；若已有运行中的实例则退出
exec 200>"$lockfile"
flock -n 200 || { echo "[$(date '+%F %T')] Another instance is running. Exit." ; exit 1; }

# 设置日志目录
mkdir -p "$log_dir"

# 数据库路径
DB_PATH="$result_dir/Sqlite/$region_id/video_details.db"
DB_WITH_TYPE_PATH="$result_dir/Sqlite/$region_id/video_details_with_type.db"
# 脚本路径
SCRIPT_SPIDER_SQLITE="$project_dir/Code/1.spider_video_details_to_sqlite.py"

# 无限循环：每轮运行任务 + 休息10分钟
while true; do
  # 获取当前小时（24小时制）
  current_hour=$(date +%H)

  if [ "$current_hour" -lt 8 ]; then
    # 如果是 0 点到 8 点之间，end_date 为去年今天的前一个月
    end_date=$(date -d "$(date -d '1 year ago' +%Y-%m-%d) -1 month" +%Y-%m-%d)
  else
    # 否则，end_date 为 5 天前
    end_date=$(date -d '5 days ago' +%Y-%m-%d)
  fi

  # 生成日志文件名
  TIME_STAMP=$(date '+%Y%m%d_%H%M%S')
  FILE_LOG="$log_dir/${TIME_STAMP}.log"

  echo "[$(date '+%F %T')] Starting new round. end_date=${end_date}" | tee "$FILE_LOG"

  # =================== 1. 抓取视频数据并存入 SQLite 数据库 ===================
  # 执行 Python 脚本并输出日志
  python "$SCRIPT_SPIDER_SQLITE" $region_id \
    --video_details_db "$DB_PATH" \
    --video_details_with_type_db "$DB_WITH_TYPE_PATH" \
    --max_pages $max_pages \
    --interval $interval \
    --end_date "$end_date" \
    >> "$FILE_LOG" 2>&1

  echo "[$(date '+%F %T')] Task finished. Sleeping for 10 minute..." >> "$FILE_LOG"

  # 休息10分钟（600秒）
  sleep 600
done
