#!/bin/bash
# ============================================
# 功能：统计本日、本周、本月、本年的视频信息
# 参数：第1个参数为 SQLite 数据库路径
# 输出：分别保存为 today.xlsx, week.xlsx, month.xlsx, year.xlsx
# ============================================

# 检查输入参数
if [ $# -lt 1 ]; then
    echo "用法：$0 <数据库路径>"
    exit 1
fi

DB_PATH="$1"
SCRIPT="/data2/luyz/project/CustomizedAnalysis/CustomizedAnalysis.12.20250726_YASbilibili/Test/get_video_data.py"   # 替换为你的 Python 脚本名

# 获取日期边界
TODAY_START=$(date +%F)
WEEK_START=$(date -d "$TODAY_START -$(($(date +%u)-1)) days" +%F)
MONTH_START=$(date +%Y-%m-01)
YEAR_START=$(date +%Y-01-01)

# 输出目录
OUTDIR="video_stats_output"
mkdir -p "$OUTDIR"

# 本日
python3 "$SCRIPT" "$DB_PATH" "$OUTDIR/today.xlsx" \
  --start "$TODAY_START" \
  --sort_by view \
  --desc \
  --topN 100

# 本周
python3 "$SCRIPT" "$DB_PATH" "$OUTDIR/week.xlsx" \
  --start "$WEEK_START" \
  --sort_by view \
  --desc \
  --topN 500

# 本月
python3 "$SCRIPT" "$DB_PATH" "$OUTDIR/month.xlsx" \
  --start "$MONTH_START" \
  --sort_by view \
  --desc \
  --topN 1000

# 本年
python3 "$SCRIPT" "$DB_PATH" "$OUTDIR/year.xlsx" \
  --start "$YEAR_START" \
  --sort_by view \
  --desc \
  --topN 5000

echo "✅ 所有统计已完成，文件保存在 $OUTDIR/ 目录下"
