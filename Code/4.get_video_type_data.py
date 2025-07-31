#!/usr/bin/env python3
# coding=utf-8
'''
Author       : luyz
Date         : 2025-07-26 21:39:40
LastEditors  : luyz
LastEditTime : 2025-07-31 20:38:13
Description  : 读取 SQLite 数据库文件并统计指定时间范围和类型的视频信息
Copyright (c) 2025 by LuYanzhuan lyanzhuan@gmail.com, All Rights Reserved.
'''

import argparse
import sqlite3
import sys
import os
import pandas as pd

def get_db_top(db_path, top_n, sort_by, descending, table_name, start_time=None, end_time=None, type_filter=None):
    # 检查数据库路径
    if not os.path.isfile(db_path):
        print(f"❌ 文件不存在：{db_path}")
        sys.exit(1)

    # 尝试连接数据库
    try:
        conn = sqlite3.connect(db_path)
    except Exception as e:
        print(f"❌ 无法连接数据库: {e}")
        sys.exit(1)

    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cursor.fetchall()]
    if not tables:
        print("❌ 数据库中没有任何表")
        conn.close()
        sys.exit(1)

    # 检查表名
    if table_name:
        if table_name not in tables:
            print(f"❌ 表 `{table_name}` 不存在。可选表有: {', '.join(tables)}")
            conn.close()
            sys.exit(1)
    else:
        table_name = tables[0]
        print(f"📦 默认读取第一个表: `{table_name}`")

    # 检查字段存在性
    try:
        preview = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 1", conn)
    except Exception as e:
        print(f"❌ 预览数据失败: {e}")
        conn.close()
        sys.exit(1)

    order_clause = ""
    if sort_by and sort_by in preview.columns:
        order_clause = f"ORDER BY `{sort_by}` {'DESC' if descending else 'ASC'}"
    elif sort_by:
        print(f"⚠️ 排序列 `{sort_by}` 不存在，忽略排序")

    # 构建 WHERE 条件
    where_conditions = []

    if 'pub_timestamp' in preview.columns:
        if start_time:
            try:
                start_ts = int(pd.to_datetime(start_time).timestamp())
                where_conditions.append(f"pub_timestamp >= {start_ts}")
            except Exception as e:
                print(f"⚠️ 开始时间解析失败，忽略: {e}")
        if end_time:
            try:
                end_ts = int(pd.to_datetime(end_time).timestamp())
                where_conditions.append(f"pub_timestamp <= {end_ts}")
            except Exception as e:
                print(f"⚠️ 结束时间解析失败，忽略: {e}")
    else:
        print("⚠️ 列 `pub_timestamp` 不存在，跳过时间过滤")

    if type_filter:
        if 'type' in preview.columns:
            where_conditions.append(f"type = '{type_filter}'")
        else:
            print("⚠️ 列 `type` 不存在，忽略类型过滤")

    # 组合 SQL
    sql = f"SELECT * FROM {table_name}"
    if where_conditions:
        sql += " WHERE " + " AND ".join(where_conditions)
    if order_clause:
        sql += " " + order_clause
    if top_n is not None:
        sql += f" LIMIT {top_n}"

    try:
        df = pd.read_sql_query(sql, conn)
    except Exception as e:
        print(f"❌ 查询数据失败: {e}")
        conn.close()
        sys.exit(1)
    finally:
        conn.close()

    return df

def format_output(df, output_path="result.xlsx"):
    if df.empty:
        print("（无数据）")
        return

    resultColumns = ["视频ID", "标题", "频道名称", "频道ID", "发布时间", "播放数", "点赞数", "评论数", "弹幕数", "收藏数", "投币数", "分享数", "简介", "封面", "时长", "标签", "视频链接", "采集时间", "分区ID", "类型", "粉丝数（采集时）"]
    df.columns = resultColumns

    if '发布时间' in df.columns:
        df['发布时间'] = pd.to_datetime(df['发布时间'], unit='s', errors='coerce')
    if '采集时间' in df.columns:
        df['采集时间'] = pd.to_datetime(df['采集时间'], unit='s', errors='coerce')

    if '时长' in df.columns:
        def format_duration(seconds):
            if pd.isna(seconds):
                return ""
            seconds = int(seconds)
            h, m = divmod(seconds, 3600)
            m, s = divmod(m, 60)
            return f"{h:02}:{m:02}:{s:02}" if h > 0 else f"{m:02}:{s:02}"
        df['时长'] = df['时长'].apply(format_duration)

    sorted_columns = [
        "标题", "时长", "频道名称", "发布时间", "粉丝数（采集时）", "播放数", "点赞数",
        "评论数", "弹幕数", "收藏数", "投币数", "分享数", "简介", "视频链接", "封面",
        "标签", "视频ID", "分区ID", "频道ID", "采集时间"
    ]
    df = df[sorted_columns]

    try:
        df.to_excel(output_path, index=False)
        print(f"✅ 数据已保存到: {output_path}")
    except Exception as e:
        print(f"❌ 写入文件失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="查看 SQLite .db 文件中的内容（可按时间和类型过滤）")
    parser.add_argument("db_path", type=str, help=".db 文件路径")
    parser.add_argument("output", type=str, help="输出文件路径")
    parser.add_argument("--start", type=str, default=None, help="起始时间（格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS）")
    parser.add_argument("--end", type=str, default=None, help="结束时间（格式同上）")
    parser.add_argument("--topN", type=int, default=100, help="显示前 N 行（默认前100行）")
    parser.add_argument("--sort_by", type=str, default=None, help="按某列排序")
    parser.add_argument("--desc", action="store_true", help="是否按降序排序")
    parser.add_argument("--table", type=str, default=None, help="指定读取的表名")
    parser.add_argument("--type_filter", type=str, default=None, help="按类型筛选（type列）")

    args = parser.parse_args()

    result = get_db_top(
        db_path=args.db_path,
        top_n=args.topN,
        sort_by=args.sort_by,
        descending=args.desc,
        table_name=args.table,
        start_time=args.start,
        end_time=args.end,
        type_filter=args.type_filter
    )

    format_output(result, args.output)
