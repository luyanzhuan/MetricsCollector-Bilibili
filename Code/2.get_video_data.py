#!/usr/bin/env python3
# coding=utf-8
'''
Author       : luyz
Date         : 2025-07-26 21:39:40
LastEditors  : luyz
LastEditTime : 2025-07-30 00:01:40
Description  : 读取 SQLite 数据库文件并统计指定时间范围内的视频信息
Copyright (c) 2025 by LuYanzhuan lyanzhuan@gmail.com, All Rights Reserved.
'''

import argparse
import sqlite3
import sys
import os
import pandas as pd

# 函数：从 SQLite 数据库中获取指定表的前 N 行数据
# 支持按时间范围过滤、排序和指定表名
# 参数：
# - db_path: 数据库文件路径
# - top_n: 返回的行数，默认为 100
# - sort_by: 按指定列排序
# - descending: 是否降序排序，默认为升序
# - table_name: 指定要查询的表名，默认为第一个表
# - start_time: 起始时间，格式为 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS
# - end_time: 结束时间，格式同上
# 返回值：一个 Pandas DataFrame，包含查询结果
def get_db_top(db_path, top_n, sort_by, descending, table_name, start_time=None, end_time=None):
    # 数据库文件存在性检查
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
    # 处理表名参数
    if table_name:
        if table_name not in tables:
            print(f"❌ 表 `{table_name}` 不存在。可选表有: {', '.join(tables)}")
            conn.close()
            sys.exit(1)
    else:
        table_name = tables[0]
        print(f"📦 默认读取第一个表: `{table_name}`")

    # 构建排序子句
    order_clause = ""
    if sort_by:
        try:
            preview = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 1", conn)
            if sort_by not in preview.columns:
                print(f"⚠️ 列 `{sort_by}` 不存在，忽略排序")
            else:
                order_clause = f"ORDER BY `{sort_by}` {'DESC' if descending else 'ASC'}"
        except Exception as e:
            print(f"⚠️ 排序检查失败，忽略排序: {e}")

    # 构建时间过滤子句
    where_clause = ""
    start_ts = None
    end_ts = None
    if start_time or end_time:
        # 检查时间列是否存在于表中
        try:
            preview = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 1", conn)
        except Exception as e:
            print(f"⚠️ 时间过滤检查失败，忽略时间过滤: {e}")
        else:
            if 'pub_timestamp' not in preview.columns:
                print("⚠️ 列 `pub_timestamp` 不存在，忽略时间过滤")
            else:
                if start_time:
                    try:
                        st_dt = pd.to_datetime(start_time)
                        start_ts = int(st_dt.timestamp())
                    except Exception as e:
                        print(f"⚠️ 开始时间解析失败，忽略开始时间: {e}")
                        start_ts = None
                if end_time:
                    try:
                        et_dt = pd.to_datetime(end_time)
                        end_ts = int(et_dt.timestamp())
                    except Exception as e:
                        print(f"⚠️ 结束时间解析失败，忽略结束时间: {e}")
                        end_ts = None
                # 根据有效的时间戳构造WHERE子句
                if start_ts is not None or end_ts is not None:
                    if start_ts is not None and end_ts is not None:
                        if start_ts > end_ts:
                            print("⚠️ 开始时间晚于结束时间，已交换两者")
                            start_ts, end_ts = end_ts, start_ts
                        where_clause = f"WHERE pub_timestamp BETWEEN {start_ts} AND {end_ts}"
                    elif start_ts is not None:
                        where_clause = f"WHERE pub_timestamp >= {start_ts}"
                    elif end_ts is not None:
                        where_clause = f"WHERE pub_timestamp <= {end_ts}"

    # 构造最终SQL查询语句
    sql = f"SELECT * FROM {table_name}"
    if where_clause:
        sql += " " + where_clause
    if order_clause:
        sql += " " + order_clause
    if top_n is not None:
        sql += f" LIMIT {top_n}"

    # 执行查询并获取数据
    try:
        df = pd.read_sql_query(sql, conn)
    except Exception as e:
        print(f"❌ 读取数据失败: {e}")
        conn.close()
        sys.exit(1)
    finally:
        conn.close()

    return df

# 整理输出结果
def format_output(df, output_path="result.xlsx"):
    if df.empty:
        print("（无数据）")
    else:
        # 修改列名
        resultColumns = ["视频ID", "标题", "频道名称", "频道ID", "发布时间", "播放数", "点赞数", "评论数", "弹幕数", "收藏数", "投币数", "分享数", "简介", "封面", "时长", "标签", "视频链接", "采集时间", "分区ID"]
        df.columns = resultColumns
        # 转换时间戳为时间格式
        if '发布时间' in df.columns:
            df['发布时间'] = pd.to_datetime(df['发布时间'], unit='s', errors='coerce')
        if '采集时间' in df.columns:
            df['采集时间'] = pd.to_datetime(df['采集时间'], unit='s', errors='coerce')
        # # 转换封面链接格式【=IMAGE("url")】
        # if '封面' in df.columns:
        #     df['封面'] = df['封面'].apply(lambda x: f'=IMAGE("{x}")' if pd.notna(x) else "")
        # 转换时长【原来是秒数，转换为 HH:MM:SS 格式，不到1小时不显示小时】
        if '时长' in df.columns:
            def format_duration(seconds):
                if pd.isna(seconds):
                    return ""
                seconds = int(seconds)
                hours, remainder = divmod(seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                if hours > 0:
                    return f"{hours:02}:{minutes:02}:{seconds:02}"
                else:
                    return f"{minutes:02}:{seconds:02}"
            df['时长'] = df['时长'].apply(format_duration)
        # 跟换列顺序
        sorted_columns = [
            "视频ID", "标题", "时长", "频道名称", "发布时间", "播放数", "点赞数",
            "评论数", "弹幕数", "收藏数", "投币数", "分享数", "简介", "视频链接", "封面",
            "标签", "视频ID", "分区ID", "频道ID", "采集时间"
        ]
        df = df[sorted_columns]
        
        # 输出到Excel文件
        try:
            df.to_excel(output_path, index=False)
            print(f"✅ 数据已保存到: {output_path}")
        except Exception as e:
            print(f"❌ 写入文件失败: {e}")
            sys.exit(1)

if __name__ == "__main__":

    # 解析命令行参数
    parser = argparse.ArgumentParser(description="查看 SQLite .db 文件中的内容（可按时间范围过滤）")
    parser.add_argument("db_path", type=str, help=".db 文件路径")
    parser.add_argument("output", type=str, help="输出文件路径")
    parser.add_argument("--start", type=str, default=None, help="起始时间（格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS）")
    parser.add_argument("--end", type=str, default=None, help="结束时间（格式同上）")
    parser.add_argument("--topN", type=int, default=100, help="显示前 N 行（默认前100行）")
    parser.add_argument("--sort_by", type=str, default=None, help="按某列排序")
    parser.add_argument("--desc", action="store_true", help="是否按降序排序")
    parser.add_argument("--table", type=str, default=None, help="指定读取的表名")
    args = parser.parse_args()

    # 调用函数获取数据
    result = get_db_top(args.db_path, args.topN, args.sort_by, args.desc, args.table, args.start, args.end)
    # 输出结果
    format_output(result, args.output)
    
