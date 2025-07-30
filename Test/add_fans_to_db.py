#!/usr/bin/env python
# coding=utf-8
'''
FilePath     : /luyz/project/CustomizedAnalysis/CustomizedAnalysis.12.20250726_YASbilibili/Test/add_fans_to_db.py
Author       : luyz
Date         : 2025-07-29 16:40:29
LastEditors  : luyz
LastEditTime : 2025-07-29 16:53:25
Description  : 
Copyright (c) 2025 by luyz && luyz@aptbiotech.com, All Rights Reserved. 
'''
# 导入所需的库
import pandas as pd
import sqlite3
import requests

# 1. 连接并读取 SQLite 数据库文件 videos.db 中的 videos 表
# 使用 sqlite3 连接数据库，再用 pandas.read_sql_query 读取整个表
conn = sqlite3.connect('/data2/luyz/project/CustomizedAnalysis/CustomizedAnalysis.12.20250726_YASbilibili/Data/Sqlite/201/video_details_with_type.db')
try:
    df = pd.read_sql_query("SELECT * FROM video_types", conn)
finally:
    conn.close()  # 读取完数据后关闭数据库连接

# 2. 提取 videos 表中的所有 up_id，去重得到唯一 UP 主 ID 列表
up_ids = df['up_id'].drop_duplicates().tolist()  # 转为列表，方便后续迭代

# 3. 查询每个 up_id 对应的当前粉丝数量
# 定义 Bilibili 粉丝数查询 API 的 URL 模板
api_url_template = "https://api.bilibili.com/x/relation/stat?vmid={}"

# 准备请求时需要的 headers，添加 User-Agent 和 Referer 防止返回 412
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
    "Referer": "https://www.bilibili.com/"
}

# 创建一个字典用于存储 up_id 到 粉丝数 的映射
follower_dict = {}

print(f"正在查询 {len(up_ids)} 个 UP 主的粉丝数...")
number = 0
for uid in up_ids[:10]:  # 仅查询前10个 UP 主，避免请求过多
    # 对每个 UID 进行请求，并捕获可能的异常
    try:
        url = api_url_template.format(uid)
        response = requests.get(url, headers=headers, timeout=5)  # 设置超时避免长时间等待
        data = response.json()  # 解析返回的 JSON 数据
        # 检查返回码和必要字段
        if data.get('code') == 0 and 'data' in data and 'follower' in data['data']:
            follower_count = data['data']['follower']
        else:
            # 若 code 非0或没有预期字段，视为获取失败
            follower_count = None
    except Exception as e:
        # 请求出错（网络异常或 JSON 解析失败等），将粉丝数置为 None 并跳过
        follower_count = None
    # 将结果存入字典
    follower_dict[uid] = follower_count
    print(f"正在查询 UP 主 {uid} 的粉丝数:{follower_count if follower_count is not None else '查询失败'}")
    print(f"已查询 {number + 1}/{len(up_ids)} 个 UP 主的粉丝数")
    number += 1

# 4. 将粉丝数量作为新列加入原始 DataFrame
# 利用 map 函数按照 up_id 匹配粉丝数，如果某 up_id 查询失败则填入 NaN
df['follower'] = df['up_id'].map(follower_dict)

# 5. 创建新的 SQLite 数据库文件 videos_with_fans.db，并写入包含新列的数据
new_conn = sqlite3.connect('videos_with_fans.db')
try:
    # 将 DataFrame 写入 SQLite，新表命名为 video_types（如果已存在则替换）
    df.to_sql('video_types', new_conn, if_exists='replace', index=False)
finally:
    new_conn.close()

# 输出成功写入后的提示信息
print("数据已成功写入新的数据库文件 video_types.db")
