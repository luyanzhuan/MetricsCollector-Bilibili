#!/bin/bash
# ============================================
# 功能：统计本日、本周、本月、本年的视频信息
# 参数：第1个参数为 SQLite 数据库路径
# 输出：分别保存为 today.xlsx, week.xlsx, month.xlsx, year.xlsx
# ============================================

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
echo "App ID：$app_id"
echo "App Secret：$app_secret"
echo "Spreadsheet Toke(Type)：$spreadsheet_type_token"
echo "Sheet ID(1 Day)：$sheet_id_1_day"
echo "Sheet ID(3 Day)：$sheet_id_3_day"
echo "Sheet ID(7 Day)：$sheet_id_7_day"
echo "Sheet ID(30 Day)：$sheet_id_30_day"
echo "Sheet ID(90 Day)：$sheet_id_90_day"
echo "Sheet ID(360 Day)：$sheet_id_360_day"

DB_WITH_TYPE_PATH="$result_dir/Sqlite/$region_id/video_details_with_type.db"
SCRIPT_WRITE_FEISHU="$project_dir/Code/3.write_excel_to_feishu.py"
SCRIPT_GET_TYPE_DATA="$project_dir/Code/4.get_video_type_data.py"

# 输出目录
OUTDIR_EXCEL="$result_dir/Excel"
mkdir -p "$OUTDIR_EXCEL"

# 1天
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
# 3天
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
# 7天
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
# 30天
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
# 90天
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
# 360天
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

echo "✅ 所有统计已完成，文件保存在 $OUTDIR_EXCEL/ 目录下"
