#!/usr/bin/env python
# coding=utf-8
'''
FilePath     : /luyz/project/CustomizedAnalysis/CustomizedAnalysis.12.20250726_YASbilibili/Test/fill_fans_to_db.py
Author       : luyz
Date         : 2025-07-30
Description  : 为 follower 列为空的行填充粉丝数（通过 up_id 调用 Bilibili API 查询）
'''

import pandas as pd
import sqlite3
import requests

# ==== 配置 ====
DB_PATH = '/data2/luyz/project/CustomizedAnalysis/CustomizedAnalysis.12.20250726_YASbilibili/Data/Sqlite/201/video_details_with_type.db'
TABLE_NAME = 'video_types'
API_TEMPLATE = "https://api.bilibili.com/x/relation/stat?vmid={}"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
    "Referer": "https://www.bilibili.com/"
}

# ==== 步骤 1：读取数据库 ====
conn = sqlite3.connect(DB_PATH)
try:
    df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)
finally:
    conn.close()

# ==== 步骤 2：找出 follower 为 NaN 的行 ====
to_update_df = df[df['follower'].isna() & df['up_id'].notna()]
up_ids = to_update_df['up_id'].drop_duplicates().tolist()

print(f"需要更新的 up_id 数量：{len(up_ids)}")

# ==== 步骤 3：调用 API 查询粉丝数 ====
follower_dict = {}
for idx, uid in enumerate(up_ids):
    try:
        url = API_TEMPLATE.format(uid)
        response = requests.get(url, headers=HEADERS, timeout=5)
        json_data = response.json()
        if json_data.get("code") == 0 and "data" in json_data:
            follower_dict[uid] = json_data["data"].get("follower", None)
        else:
            follower_dict[uid] = None
    except Exception as e:
        follower_dict[uid] = None
    print(f"[{idx+1}/{len(up_ids)}] 查询 {uid} -> {follower_dict[uid]}")
    # 可加延迟避免封禁：time.sleep(0.5)

# ==== 步骤 4：更新原 DataFrame ====
df.loc[df['follower'].isna() & df['up_id'].isin(follower_dict.keys()), 'follower'] = \
    df.loc[df['follower'].isna() & df['up_id'].isin(follower_dict.keys()), 'up_id'].map(follower_dict)

# ==== 步骤 5：写回数据库（仅写整表，避免复杂 UPDATE）====
conn = sqlite3.connect(DB_PATH)
try:
    df.to_sql(TABLE_NAME, conn, index=False, if_exists='replace')
finally:
    conn.close()

print("✅ follower 列空值填充完成，并已写回数据库")
