#!/bin/bash

# 获取当前脚本所在的目录（支持软链接和相对路径）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 构建配置文件路径：脚本同级目录的 Config/Example/config.conf
# 这里假设配置文件位于脚本目录下的，你需要根据实际情况调整路径或者参数信息
CONFIG_FILE="${SCRIPT_DIR}/Config/Example/config.conf"

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
# 飞书相关参数
echo "App ID：$app_id"
echo "App Secret：$app_secret"
echo "Spreadsheet Token：$spreadsheet_token"
echo "Sheet ID(Day)：$sheet_id_day"
echo "Sheet ID(Week)：$sheet_id_week"
echo "Sheet ID(Month)：$sheet_id_month"
echo "Sheet ID(Year)：$sheet_id_year"
echo "Spreadsheet Toke(Type)：$spreadsheet_type_token"
echo "Sheet ID(1 Day)：$sheet_id_1_day"
echo "Sheet ID(3 Day)：$sheet_id_3_day"
echo "Sheet ID(7 Day)：$sheet_id_7_day"
echo "Sheet ID(30 Day)：$sheet_id_30_day"
echo "Sheet ID(90 Day)：$sheet_id_90_day"
echo "Sheet ID(360 Day)：$sheet_id_360_day"

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
SCRIPT_GET_DATA="$project_dir/Code/2.get_video_data.py"
SCRIPT_WRITE_FEISHU="$project_dir/Code/3.write_excel_to_feishu.py"
SCRIPT_GET_TYPE_DATA="$project_dir/Code/4.get_video_type_data.py"

# 输出目录
OUTDIR_EXCEL="$result_dir/Excel"
mkdir -p "$OUTDIR_EXCEL"

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

  # =================== 2. 获取热门视频数据并写入 Excel 和飞书表格 ===================
  # 获取日期边界
  TODAY_START=$(date +%F)
  WEEK_START=$(date -d "$TODAY_START -$(($(date +%u)-1)) days" +%F)
  MONTH_START=$(date +%Y-%m-01)
  YEAR_START=$(date +%Y-01-01)
  # 2.1 每日热门
  # 从数据库获取数据并保存为 Excel
  python3 "$SCRIPT_GET_DATA" "$DB_PATH" "$OUTDIR_EXCEL/Today.xlsx" \
    --start "$TODAY_START" \
    --sort_by view \
    --desc \
    --topN 100
  # 将 Excel 写入飞书表格
  python3 "$SCRIPT_WRITE_FEISHU" \
    --excel_path "$OUTDIR_EXCEL/Today.xlsx" \
    --sheet_name "Sheet1" \
    --app_id "$app_id" \
    --app_secret "$app_secret" \
    --spreadsheet_token "$spreadsheet_token" \
    --sheet_id "$sheet_id_day" \
    --start_cell "A1"
  # 2.2 本周热门
  # 从数据库获取数据并保存为 Excel
  python3 "$SCRIPT_GET_DATA" "$DB_PATH" "$OUTDIR_EXCEL/Week.xlsx" \
    --start "$WEEK_START" \
    --sort_by view \
    --desc \
    --topN 500
  # 将 Excel 写入飞书表格
  python3 "$SCRIPT_WRITE_FEISHU" \
    --excel_path "$OUTDIR_EXCEL/Week.xlsx" \
    --sheet_name "Sheet1" \
    --app_id "$app_id" \
    --app_secret "$app_secret" \
    --spreadsheet_token "$spreadsheet_token" \
    --sheet_id "$sheet_id_week" \
    --start_cell "A1"
  # 2.3 本月热门
  # 从数据库获取数据并保存为 Excel
  python3 "$SCRIPT_GET_DATA" "$DB_PATH" "$OUTDIR_EXCEL/Month.xlsx" \
    --start "$MONTH_START" \
    --sort_by view \
    --desc \
    --topN 1000
  # 将 Excel 写入飞书表格
  python3 "$SCRIPT_WRITE_FEISHU" \
    --excel_path "$OUTDIR_EXCEL/Month.xlsx" \
    --sheet_name "Sheet1" \
    --app_id "$app_id" \
    --app_secret "$app_secret" \
    --spreadsheet_token "$spreadsheet_token" \
    --sheet_id "$sheet_id_month" \
    --start_cell "A1"
  # 2.4 本年热门
  # 从数据库获取数据并保存为 Excel
  python3 "$SCRIPT_GET_DATA" "$DB_PATH" "$OUTDIR_EXCEL/Year.xlsx" \
    --start "$YEAR_START" \
    --sort_by view \
    --desc \
    --topN 3000
  # 将 Excel 写入飞书表格
  python3 "$SCRIPT_WRITE_FEISHU" \
    --excel_path "$OUTDIR_EXCEL/Year.xlsx" \
    --sheet_name "Sheet1" \
    --app_id "$app_id" \
    --app_secret "$app_secret" \
    --spreadsheet_token "$spreadsheet_token" \
    --sheet_id "$sheet_id_year" \
    --start_cell "A1"
  echo "[$(date '+%F %T')] Data collection and upload completed." >> "$FILE_LOG"

  # =================== 3. 获取固定时间长度视频统计数据并写入飞书表格 ===================
  # 3.1 1天
  # 从数据库获取数据并保存为 Excel
  python3 "$SCRIPT_GET_TYPE_DATA" "$DB_WITH_TYPE_PATH" "$OUTDIR_EXCEL/1_Day.xlsx" \
    --type_filter "1_day" \
    --sort_by view \
    --desc \
    --topN 100
  # 将 Excel 写入飞书表格
  python3 "$SCRIPT_WRITE_FEISHU" \
    --excel_path "$OUTDIR_EXCEL/1_Day.xlsx" \
    --sheet_name "Sheet1" \
    --app_id "$app_id" \
    --app_secret "$app_secret" \
    --spreadsheet_token "$spreadsheet_type_token" \
    --sheet_id "$sheet_id_1_day" \
    --start_cell "A1"
  # 3.3 3天
  # 从数据库获取数据并保存为 Excel
  python3 "$SCRIPT_GET_TYPE_DATA" "$DB_WITH_TYPE_PATH" "$OUTDIR_EXCEL/3_Day.xlsx" \
    --type_filter "3_day" \
    --sort_by view \
    --desc \
    --topN 100
  # 将 Excel 写入飞书表格
  python3 "$SCRIPT_WRITE_FEISHU" \
    --excel_path "$OUTDIR_EXCEL/3_Day.xlsx" \
    --sheet_name "Sheet1" \
    --app_id "$app_id" \
    --app_secret "$app_secret" \
    --spreadsheet_token "$spreadsheet_type_token" \
    --sheet_id "$sheet_id_3_day" \
    --start_cell "A1"
  # 3.3 7天
  # 从数据库获取数据并保存为 Excel
  python3 "$SCRIPT_GET_TYPE_DATA" "$DB_WITH_TYPE_PATH" "$OUTDIR_EXCEL/7_Day.xlsx" \
    --type_filter "7_day" \
    --sort_by view \
    --desc \
    --topN 100
  # 将 Excel 写入飞书表格
  python3 "$SCRIPT_WRITE_FEISHU" \
    --excel_path "$OUTDIR_EXCEL/7_Day.xlsx" \
    --sheet_name "Sheet1" \
    --app_id "$app_id" \
    --app_secret "$app_secret" \
    --spreadsheet_token "$spreadsheet_type_token" \
    --sheet_id "$sheet_id_7_day" \
    --start_cell "A1"
  # 3.4 30天
  # 从数据库获取数据并保存为 Excel
  python3 "$SCRIPT_GET_TYPE_DATA" "$DB_WITH_TYPE_PATH" "$OUTDIR_EXCEL/30_Day.xlsx" \
    --type_filter "30_day" \
    --sort_by view \
    --desc \
    --topN 100
  # 将 Excel 写入飞书表格
  python3 "$SCRIPT_WRITE_FEISHU" \
    --excel_path "$OUTDIR_EXCEL/30_Day.xlsx" \
    --sheet_name "Sheet1" \
    --app_id "$app_id" \
    --app_secret "$app_secret" \
    --spreadsheet_token "$spreadsheet_type_token" \
    --sheet_id "$sheet_id_30_day" \
    --start_cell "A1"
  # 3.5 90天
  # 从数据库获取数据并保存为 Excel
  python3 "$SCRIPT_GET_TYPE_DATA" "$DB_WITH_TYPE_PATH" "$OUTDIR_EXCEL/90_Day.xlsx" \
    --type_filter "90_day" \
    --sort_by view \
    --desc \
    --topN 100
  # 将 Excel 写入飞书表格
  python3 "$SCRIPT_WRITE_FEISHU" \
    --excel_path "$OUTDIR_EXCEL/90_Day.xlsx" \
    --sheet_name "Sheet1" \
    --app_id "$app_id" \
    --app_secret "$app_secret" \
    --spreadsheet_token "$spreadsheet_type_token" \
    --sheet_id "$sheet_id_90_day" \
    --start_cell "A1"
  # 3.6 360天
  # 从数据库获取数据并保存为 Excel
  python3 "$SCRIPT_GET_TYPE_DATA" "$DB_WITH_TYPE_PATH" "$OUTDIR_EXCEL/360_Day.xlsx" \
    --type_filter "360_day" \
    --sort_by view \
    --desc \
    --topN 100
  # 将 Excel 写入飞书表格
  python3 "$SCRIPT_WRITE_FEISHU" \
    --excel_path "$OUTDIR_EXCEL/360_Day.xlsx" \
    --sheet_name "Sheet1" \
    --app_id "$app_id" \
    --app_secret "$app_secret" \
    --spreadsheet_token "$spreadsheet_type_token" \
    --sheet_id "$sheet_id_360_day" \
    --start_cell "A1"
  echo "[$(date '+%F %T')] Type data collection and upload completed." >> "$FILE_LOG"

  echo "[$(date '+%F %T')] Task finished. Sleeping for 10 minute..." >> "$FILE_LOG"

  # 休息10分钟（60秒）
  sleep 60
done
