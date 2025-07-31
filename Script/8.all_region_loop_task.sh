#!/bin/bash

# 获取当前脚本所在的目录（支持软链接和相对路径）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 构建配置文件路径：脚本同级目录的 Config/Example
# 这里假设配置文件位于脚本目录下的，你需要根据实际情况调整路径或者参数信息
CONFIG_DIR="${SCRIPT_DIR}/Config/Example"
CONFIG_FILE="${CONFIG_DIR}/config.conf"
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


# 遍历一级目录下的所有文件（不包含子目录中的文件）
for dir in "${CONFIG_DIR}"/*; do
  if [[ -d "$dir" ]]; then

    # 获取当前一级目录的目录名（去掉路径）
    REGION_ID=$(basename "$dir")

    # 构建当前分区的配置文件路径
    CURRENT_REGION_CONFIG_FILE="${dir}/config.conf"
    if [ ! -f "$CURRENT_REGION_CONFIG_FILE" ]; then
      echo "❌ 配置文件不存在: $CURRENT_REGION_CONFIG_FILE"
      continue
    fi

    mkdir -p "$log_dir/$REGION_ID"
    nohup $project_dir/Script/7.loop_run_metrics_collector_and_stats.sh "$CURRENT_REGION_CONFIG_FILE" > "$log_dir/$REGION_ID/loop_runner_stdout.log" 2>&1 &
    echo "✅ 启动分区 $REGION_ID 的任务，日志输出到 $log_dir/$REGION_ID/loop_runner_stdout.log"

  fi
done
