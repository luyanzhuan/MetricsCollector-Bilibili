#!/usr/bin/env python
# coding=utf-8
"""
File        : deduplicate_by_pk.py
Description : 输出主键字段并根据主键对指定 SQLite 表去重，覆盖原表
Usage       : python deduplicate_by_pk.py <db_path> <table_name>
"""

import argparse
import sqlite3
import pandas as pd
import os

def get_primary_key_columns(db_path, table_name):
    """获取表的主键列名（支持联合主键）"""
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        pk_columns = [row[1] for row in cursor.fetchall() if row[5] > 0]  # 第6列是 pk，>0 表示是主键
    finally:
        conn.close()

    if not pk_columns:
        print(f"⚠️ 警告：表 {table_name} 没有定义主键列！")
    else:
        print(f"✅ 表 {table_name} 的主键列为：{pk_columns}")
    return pk_columns

def deduplicate_table(db_path, table_name, pk_columns):
    """读取表并根据主键列去重，覆盖原始表"""
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

        if not pk_columns:
            print("❌ 无法去重：主键列为空！请先定义主键。")
            return

        # 去重（保留主键组合唯一的第一条记录）
        before_rows = len(df)
        df = df.drop_duplicates(subset=pk_columns, keep='first')
        after_rows = len(df)

        print(f"✅ 去重完成：从 {before_rows} 条记录变为 {after_rows} 条记录")

        # 覆盖写入原表
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"✅ 已覆盖写入表 {table_name}")
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="输出 SQLite 表主键并根据主键去重")
    parser.add_argument("db_path", type=str, help="SQLite 数据库文件路径")
    parser.add_argument("table_name", type=str, help="表名")

    args = parser.parse_args()

    if not os.path.exists(args.db_path):
        print(f"❌ 数据库文件不存在: {args.db_path}")
    else:
        pk_columns = get_primary_key_columns(args.db_path, args.table_name)
        deduplicate_table(args.db_path, args.table_name, pk_columns)
