#!/usr/bin/env python
# coding=utf-8
'''
Author       : luyz
Date         : 2025-07-31 20:53:12
LastEditors  : luyz
LastEditTime : 2025-07-31 20:57:41
Description  : 从现有 SQLite 表复制数据，指定列作为主键并创建新表
Copyright (c) 2025 by LuYanzhuan lyanzhuan@gmail.com, All Rights Reserved.
'''

import argparse
import sqlite3
import pandas as pd
import os

def get_table_schema(conn, table_name):
    """获取表结构：列名和类型"""
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    schema = [(row[1], row[2]) for row in cursor.fetchall()]
    return schema

def generate_create_sql(table_name, schema, pk_columns):
    """生成包含主键定义的 CREATE TABLE SQL"""
    column_defs = [f"{col} {dtype}" for col, dtype in schema]
    pk_clause = f"PRIMARY KEY ({', '.join(pk_columns)})"
    create_sql = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(column_defs) + f",\n  {pk_clause}\n)"
    return create_sql

def auto_generate_output_path(input_db_path):
    """自动构造新数据库路径：添加 _with_pk 后缀"""
    base, ext = os.path.splitext(input_db_path)
    return f"{base}_with_pk{ext}"

def main():
    parser = argparse.ArgumentParser(description="为表添加主键并复制到新 SQLite 文件（自动路径）")
    parser.add_argument("input_db", type=str, help="原 SQLite 数据库文件路径")
    parser.add_argument("table_name", type=str, help="表名")
    parser.add_argument("pk_columns", type=str, nargs="+", help="主键列名（可多个）")

    args = parser.parse_args()
    input_db = args.input_db
    table_name = args.table_name
    pk_columns = args.pk_columns

    if not os.path.exists(input_db):
        print(f"❌ 输入数据库文件不存在: {input_db}")
        return

    # 自动构造输出路径
    output_db = auto_generate_output_path(input_db)
    print(f"📁 新数据库文件将保存为: {output_db}")

    # 连接原数据库，提取表结构和数据
    in_conn = sqlite3.connect(input_db)
    try:
        schema = get_table_schema(in_conn, table_name)
        schema_columns = [col[0] for col in schema]
        for pk in pk_columns:
            if pk not in schema_columns:
                raise ValueError(f"❌ 主键列 '{pk}' 不在表字段中！字段包括: {schema_columns}")

        df = pd.read_sql_query(f"SELECT * FROM {table_name}", in_conn)
        df = df.drop_duplicates(subset=pk_columns)
        print(f"✅ 从表 {table_name} 读取数据，共 {len(df)} 条记录（去重后）")
    finally:
        in_conn.close()

    # 删除已有目标文件
    if os.path.exists(output_db):
        print(f"⚠️ 目标数据库已存在，删除旧文件: {output_db}")
        os.remove(output_db)

    # 写入新数据库
    out_conn = sqlite3.connect(output_db)
    try:
        create_sql = generate_create_sql(table_name, schema, pk_columns)
        out_conn.execute(create_sql)
        out_conn.commit()
        df.to_sql(table_name, out_conn, if_exists="append", index=False)
        print(f"✅ 新数据库创建成功：{output_db}")
        print(f"✅ 表 {table_name} 主键列为：{pk_columns}")
    finally:
        out_conn.close()

if __name__ == "__main__":
    main()