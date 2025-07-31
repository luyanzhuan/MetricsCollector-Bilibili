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

# 获取去年今天的前一个月的日期（格式：YYYY-MM-DD）
end_date=$(date -d "$(date -d '1 year ago' +%Y-%m-%d) -1 month" +%Y-%m-%d)

# 创建日志目录（如果不存在）
mkdir -p "$log_dir"

# 日志文件名（使用当前时间命名，精确到秒）
log_file="$log_dir/$(date '+%Y%m%d_%H%M%S').log"

# 执行 Python 脚本并记录日志
python $project_dir/Code/1.spider_video_details_to_sqlite.py $region_id \
  --video_details_db $result_dir/Sqlite/$region_id/video_details.db \
  --video_details_with_type_db $result_dir/Sqlite/$region_id/video_details_with_type.db \
  --max_pages $max_pages \
  --interval $interval \
  --end_date "$end_date" \
  &> "$log_file"