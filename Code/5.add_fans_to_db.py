#!/usr/bin/env python
# coding=utf-8
'''
Author       : luyz
Date         : 2025-07-26 22:52:28
LastEditors  : luyz
LastEditTime : 2025-07-31 20:15:56
Description  : 给 SQLite 中指定表添加 follower 列（B站粉丝数），直接更新原数据库
Copyright (c) 2025 by LuYanzhuan lyanzhuan@gmail.com, All Rights Reserved.
'''

import pandas as pd
import sqlite3
import requests
import os
import argparse

def load_table_from_database(db_path, table_name):
    """连接数据库并读取指定表"""
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    finally:
        conn.close()
    return df

def ensure_follower_column(df):
    """如果不存在 follower 列，则添加空值列"""
    if 'follower' not in df.columns:
        df['follower'] = pd.NA
    return df

def query_follower_counts(up_ids):
    """查询 B 站粉丝数，返回 up_id 到 follower 数的字典"""
    api_url_template = "https://api.bilibili.com/x/relation/stat?vmid={}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
        "Referer": "https://www.bilibili.com/"
    }
    follower_dict = {}

    print(f"正在查询 {len(up_ids)} 个 UP 主的粉丝数...")
    for number, uid in enumerate(up_ids):
        try:
            url = api_url_template.format(uid)
            response = requests.get(url, headers=headers, timeout=5)
            data = response.json()
            if data.get('code') == 0 and 'data' in data and 'follower' in data['data']:
                follower_count = data['data']['follower']
            else:
                follower_count = None
        except Exception:
            follower_count = None
        follower_dict[uid] = follower_count
        print(f"[{number + 1}/{len(up_ids)}] UP 主 {uid} 粉丝数: {follower_count if follower_count is not None else '查询失败'}")
    return follower_dict

def update_dataframe_with_fans(df, follower_dict):
    """使用字典更新 df 的 follower 列"""
    df['follower'] = df['up_id'].map(lambda x: follower_dict.get(x, df.loc[df['up_id'] == x, 'follower'].iloc[0]))
    return df

def write_table_to_database(df, db_path, table_name):
    """覆盖写入 SQLite 文件中的指定表"""
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
    finally:
        conn.close()
    print(f"✅ 数据已更新到原始数据库: {db_path}")


# -------------------- 主程序入口 --------------------
if __name__ == "__main__":
    # -------------------- 参数解析 --------------------
    parser = argparse.ArgumentParser(description="为 SQLite 表添加 B站粉丝数（follower列）并直接更新原数据库")
    parser.add_argument("db_path", type=str, help=".db 文件路径")
    parser.add_argument("--table", type=str, required=True, help="要处理的表名")
    args = parser.parse_args()

    # -------------------- 主流程 --------------------
    if not os.path.exists(args.db_path):
        print(f"❌ 错误：数据库文件不存在: {args.db_path}")
        return

    df = load_table_from_database(args.db_path, args.table)
    df = ensure_follower_column(df)
    up_ids = df['up_id'].drop_duplicates().tolist()
    follower_dict = query_follower_counts(up_ids)
    df = update_dataframe_with_fans(df, follower_dict)
    write_table_to_database(df, args.db_path, args.table)
