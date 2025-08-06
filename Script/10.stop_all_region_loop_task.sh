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
echo "结果目录：$result_dir"
echo "日志目录：$log_dir"

# 1️⃣ 循环外统一 kill 所有脚本进程（根据脚本名）
echo "🧨 尝试查杀所有包含 loop_run_with_lock 进程..."
ps aux | grep "[l]oop_run_with_lock" | awk '{print $2}' | while read -r pid; do
  kill "$pid" && echo "✅ kill $pid（脚本进程）" || echo "❌ 无法 kill $pid"
done
echo "🧨 尝试查杀所有包含 spider_video_details_to_sqlite 进程..."
ps aux | grep "[s]pider_video_details_to_sqlite" | awk '{print $2}' | while read -r pid; do
  kill "$pid" && echo "✅ kill $pid（脚本进程）" || echo "❌ 无法 kill $pid"
done

# 遍历一级目录下的所有文件（不包含子目录中的文件）
for dir in "${CONFIG_DIR}"/*; do
  if [[ -d "$dir" ]]; then

    REGION_ID=$(basename "$dir")

    # 2️⃣ 删除DB锁文件（如 *.lock）
    for lock_file in "$result_dir/Sqlite/$REGION_ID/"*.lock; do
      if [[ -f "$lock_file" ]]; then
        rm -f "$lock_file" && echo "✅ 删除锁文件: $lock_file（$REGION_ID）" || echo "❌ 无法删除锁文件: $lock_file（$REGION_ID）"
      else
        echo "⚠️ 锁文件不存在: $lock_file"
      fi
    done

    # 3️⃣ 删除DB临时文件（如 *.db-journal）
    for temp_file in "$result_dir/Sqlite/$REGION_ID/"*-journal; do
      if [[ -f "$temp_file" ]]; then
        rm -f "$temp_file" && echo "✅ 删除临时文件: $temp_file（$REGION_ID）" || echo "❌ 无法删除临时文件: $temp_file（$REGION_ID）"
      else
        echo "⚠️ 临时文件不存在: $temp_file"
      fi
    done

  fi
done
