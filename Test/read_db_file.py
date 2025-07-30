#!/usr/bin/env python3
# coding=utf-8
'''
Author       : luyz
Date         : 2025-07-26 21:39:40
LastEditors  : luyz
LastEditTime : 2025-07-26 22:42:43
Description  : 读取 SQLite 数据库文件并显示内容
Copyright (c) 2025 by LuYanzhuan lyanzhuan@gmail.com, All Rights Reserved.
'''

import argparse
import sqlite3
import sys
import os
import pandas as pd

def view_db_top(db_path, top_n, sort_by, descending, output_path, table_name):
    if not os.path.isfile(db_path):
        print(f"❌ 文件不存在：{db_path}")
        sys.exit(1)

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

    # 表名处理
    if table_name:
        if table_name not in tables:
            print(f"❌ 表 `{table_name}` 不存在。可选表有: {', '.join(tables)}")
            conn.close()
            sys.exit(1)
    else:
        table_name = tables[0]
        print(f"📦 默认读取第一个表: `{table_name}`")

    # 判断是否加排序和限制
    order_clause = ""
    if sort_by:
        # 检查列名是否存在（先查询前1行）
        try:
            preview = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 1", conn)
            if sort_by not in preview.columns:
                print(f"⚠️ 列 `{sort_by}` 不存在，忽略排序")
            else:
                order_clause = f"ORDER BY `{sort_by}` {'DESC' if descending else 'ASC'}"
        except Exception as e:
            print(f"⚠️ 排序检查失败，忽略排序: {e}")

    # 构造 SQL
    sql = f"SELECT * FROM {table_name} {order_clause}"
    if top_n is not None:
        sql += f" LIMIT {top_n}"
    else:
        sql += " LIMIT 100" if not output_path else ""

    try:
        df = pd.read_sql_query(sql, conn)
    except Exception as e:
        print(f"❌ 读取数据失败: {e}")
        conn.close()
        sys.exit(1)
    finally:
        conn.close()

    if output_path:
        try:
            df.to_csv(output_path, index=False)
            print(f"✅ 数据已保存到: {output_path}")
        except Exception as e:
            print(f"❌ 写入文件失败: {e}")
    else:
        # 打印最多100行
        max_print = min(len(df), 100)
        print(df.head(max_print).to_string(index=False))
        if len(df) > 100:
            print(f"\n🔍 仅显示前 100 行，实际共 {len(df)} 行。")

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="查看 SQLite .db 文件中的内容")
    parser.add_argument("db_path", type=str, help=".db 文件路径")
    parser.add_argument("--topN", type=int, default=None, help="显示前 N 行（默认显示全部）")
    parser.add_argument("--sort_by", type=str, default=None, help="按某列排序")
    parser.add_argument("--desc", action="store_true", help="是否按降序排序")
    parser.add_argument("--output", type=str, default=None, help="输出文件路径（若不填则打印，最多显示 100 行）")
    parser.add_argument("--table", type=str, default=None, help="指定读取的表名")

    args = parser.parse_args()
    view_db_top(args.db_path, args.topN, args.sort_by, args.desc, args.output, args.table)

