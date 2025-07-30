#!/bin/bash

# 获取当前脚本所在的目录（支持软链接和相对路径）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 构建配置文件路径：脚本同级目录的 Config/Final/config.conf
CONFIG_FILE="${SCRIPT_DIR}/Config/Final/config.conf"

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

# 设置任务脚本路径
python_script="$project_dir/Code/1.spider_video_details_to_sqlite.py"

# 无限循环：每轮运行任务 + 休息10分钟
while true; do
  # 生成 end_date（去年今天的前一个月）
  end_date=$(date -d "$(date -d '1 year ago' +%Y-%m-%d) -1 month" +%Y-%m-%d)

  # 生成日志文件名
  timestamp=$(date '+%Y%m%d_%H%M%S')
  log_file="$log_dir/${timestamp}.log"

  echo "[$(date '+%F %T')] Starting new round. end_date=${end_date}" | tee "$log_file"

  # 执行 Python 脚本并输出日志
  python "$python_script" $region_id \
    --video_details_db "$project_dir/Data/Sqlite/$region_id/video_details.db" \
    --video_details_with_type_db "$project_dir/Data/Sqlite/$region_id/video_details_with_type.db" \
    --max_pages $max_pages \
    --interval $interval \
    --end_date "$end_date" \
    >> "$log_file" 2>&1

  echo "[$(date '+%F %T')] Task finished. Sleeping for 10 minutes..." >> "$log_file"

  # 休息10分钟（600秒）
  sleep 600
done
