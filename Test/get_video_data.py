#!/usr/bin/env python3
# coding=utf-8
'''
Author       : luyz
Date         : 2025-07-26 21:39:40
LastEditors  : luyz
LastEditTime : 2025-07-29 15:44:38
Description  : 读取 SQLite 数据库文件并统计指定时间范围内的视频信息
Copyright (c) 2025 by LuYanzhuan lyanzhuan@gmail.com, All Rights Reserved.
'''

import argparse
import sqlite3
import sys
import os
import pandas as pd

def view_db_top(db_path, top_n, sort_by, descending, output_path, table_name, start_time=None, end_time=None):
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
    else:
        sql += " LIMIT 100" if not output_path else ""

    # 执行查询并获取数据
    try:
        df = pd.read_sql_query(sql, conn)
    except Exception as e:
        print(f"❌ 读取数据失败: {e}")
        conn.close()
        sys.exit(1)
    finally:
        conn.close()

    # 输出结果到文件或控制台
    if output_path:
        try:
            # df.to_csv(output_path, index=False)
            df.to_excel(output_path, index=False)
            print(f"✅ 数据已保存到: {output_path}")
        except Exception as e:
            print(f"❌ 写入文件失败: {e}")
    else:
        max_print = min(len(df), 100)
        if max_print > 0:
            print(df.head(max_print).to_string(index=False))
        else:
            print("（无数据）")
        if len(df) > 100:
            print(f"\n🔍 仅显示前 100 行，实际共 {len(df)} 行。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="查看 SQLite .db 文件中的内容（可按时间范围过滤）")
    parser.add_argument("db_path", type=str, help=".db 文件路径")
    parser.add_argument("--start", type=str, default=None, help="起始时间（格式：YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS）")
    parser.add_argument("--end", type=str, default=None, help="结束时间（格式同上）")
    parser.add_argument("--topN", type=int, default=None, help="显示前 N 行（默认显示全部）")
    parser.add_argument("--sort_by", type=str, default=None, help="按某列排序")
    parser.add_argument("--desc", action="store_true", help="是否按降序排序")
    parser.add_argument("--output", type=str, default=None, help="输出文件路径（若不填则打印，最多显示 100 行）")
    parser.add_argument("--table", type=str, default=None, help="指定读取的表名")
    args = parser.parse_args()
    view_db_top(args.db_path, args.topN, args.sort_by, args.desc, args.output, args.table, args.start, args.end)
