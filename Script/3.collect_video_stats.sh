#!/bin/bash
# ============================================
# 功能：统计本日、本周、本月、本年的视频信息
# 参数：第1个参数为 SQLite 数据库路径
# 输出：分别保存为 today.xlsx, week.xlsx, month.xlsx, year.xlsx
# ============================================

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
echo "日志目录：$log_dir"
echo "分区ID：$region_id"
echo "App ID：$app_id"
echo "App Secret：$app_secret"
echo "Spreadsheet Token：$spreadsheet_token"
echo "Sheet ID(Day)：$sheet_id_day"
echo "Sheet ID(Week)：$sheet_id_week"
echo "Sheet ID(Month)：$sheet_id_month"
echo "Sheet ID(Year)：$sheet_id_year"

DB_PATH="$project_dir/Data/Sqlite/$region_id/video_details.db"
SCRIPT_GET_DATA="$project_dir/Code/2.get_video_data.py"
SCRIPT_WRITE_FEISHU="$project_dir/Code/3.write_excel_to_feishu.py"

# 获取日期边界
TODAY_START=$(date +%F)
WEEK_START=$(date -d "$TODAY_START -$(($(date +%u)-1)) days" +%F)
MONTH_START=$(date +%Y-%m-01)
YEAR_START=$(date +%Y-01-01)

# 输出目录
OUTDIR="$project_dir/Data/Excel"
mkdir -p "$OUTDIR"

# 本日
# 从数据库获取数据并保存为 Excel
python3 "$SCRIPT_GET_DATA" "$DB_PATH" "$OUTDIR/Today.xlsx" \
  --start "$TODAY_START" \
  --sort_by view \
  --desc \
  --topN 100
# 将 Excel 写入飞书表格
python3 "$SCRIPT_WRITE_FEISHU" \
  --excel_path "$OUTDIR/Today.xlsx" \
  --sheet_name "Sheet1" \
  --app_id "$app_id" \
  --app_secret "$app_secret" \
  --spreadsheet_token "$spreadsheet_token" \
  --sheet_id "$sheet_id_day" \
  --start_cell "A1"

# 本周
# 从数据库获取数据并保存为 Excel
python3 "$SCRIPT_GET_DATA" "$DB_PATH" "$OUTDIR/Week.xlsx" \
  --start "$WEEK_START" \
  --sort_by view \
  --desc \
  --topN 500
# 将 Excel 写入飞书表格
python3 "$SCRIPT_WRITE_FEISHU" \
  --excel_path "$OUTDIR/Week.xlsx" \
  --sheet_name "Sheet1" \
  --app_id "$app_id" \
  --app_secret "$app_secret" \
  --spreadsheet_token "$spreadsheet_token" \
  --sheet_id "$sheet_id_week" \
  --start_cell "A1"

# 本月
# 从数据库获取数据并保存为 Excel
python3 "$SCRIPT_GET_DATA" "$DB_PATH" "$OUTDIR/Month.xlsx" \
  --start "$MONTH_START" \
  --sort_by view \
  --desc \
  --topN 1000
# 将 Excel 写入飞书表格
python3 "$SCRIPT_WRITE_FEISHU" \
  --excel_path "$OUTDIR/Month.xlsx" \
  --sheet_name "Sheet1" \
  --app_id "$app_id" \
  --app_secret "$app_secret" \
  --spreadsheet_token "$spreadsheet_token" \
  --sheet_id "$sheet_id_month" \
  --start_cell "A1"

# 本年
# 从数据库获取数据并保存为 Excel
python3 "$SCRIPT_GET_DATA" "$DB_PATH" "$OUTDIR/Year.xlsx" \
  --start "$YEAR_START" \
  --sort_by view \
  --desc \
  --topN 3000
# 将 Excel 写入飞书表格
python3 "$SCRIPT_WRITE_FEISHU" \
  --excel_path "$OUTDIR/Year.xlsx" \
  --sheet_name "Sheet1" \
  --app_id "$app_id" \
  --app_secret "$app_secret" \
  --spreadsheet_token "$spreadsheet_token" \
  --sheet_id "$sheet_id_year" \
  --start_cell "A1"

echo "✅ 所有统计已完成，文件保存在 $OUTDIR/ 目录下"
