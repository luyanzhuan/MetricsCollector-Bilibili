#!/usr/bin/env python
# coding=utf-8
'''
Author       : luyz
Date         : 2025-07-26 22:52:28
LastEditors  : luyz
LastEditTime : 2025-07-27 09:31:28
Description  : 
Copyright (c) 2025 by LuYanzhuan lyanzhuan@gmail.com, All Rights Reserved.
'''

import argparse
import requests
import random
import time
import sqlite3
import os
import sys
import pandas as pd
from datetime import datetime, timedelta


# 创建数据库（视频详细信息）并初始化表格
def init_video_db(db_path):
    """
    初始化视频数据库
    如果数据库文件不存在，则创建新的 SQLite 数据库
    如果数据库文件存在，则检查是否有必要更新表结构
    """

    create_table_sql = '''
        CREATE TABLE videos (
            bvid TEXT PRIMARY KEY,
            title TEXT,
            up_name TEXT,
            up_id INTEGER,
            pub_timestamp INTEGER,
            view INTEGER,
            like INTEGER,
            reply INTEGER,
            danmaku INTEGER,
            favorite INTEGER,
            coin INTEGER,
            share INTEGER,
            description TEXT,
            cover TEXT,
            duration INTEGER,
            tag TEXT,
            video_url TEXT,
            fetch_timestamp INTEGER,
            region_id INTEGER
        )
    '''

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    if not os.path.exists(db_path):
        cursor.execute(create_table_sql)
        conn.commit()
    else:
        print(f"📦 数据库 `{db_path}` 已存在")
        # 检查表格是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='videos';")
        if not cursor.fetchone():
            print("📦 创建视频信息表格 `videos`")
            cursor.execute(create_table_sql)
            conn.commit()
        else:
            print("📦 表格 `videos` 已存在")
            # 检查是否有必要更新表结构
            cursor.execute("PRAGMA table_info(videos);")
            columns = [col[1] for col in cursor.fetchall()]
            required_columns = ['bvid', 'title', 'up_name', 'up_id',
                                'pub_timestamp', 'view', 'like', 'reply',
                                'danmaku', 'favorite', 'coin', 'share',
                                'description', 'cover', 'duration',
                                'tag', 'video_url', 'fetch_timestamp',
                                'region_id']
            for col in required_columns:
                if col not in columns:
                    print(f"⚠️ 列 `{col}` 不存在，可能需要手动更新表结构")
    conn.close()

# 创建数据库（视频详细信息带有烈性）并初始化表格
# 注意：此函数与 init_video_db 类似，但表结构不同
# type: 1_day(1天)、3_day(3天)、7_day(1周)、30_day(1月)、90_day(3月)、360_day(1年)
def init_video_type_db(db_path):
    """
    初始化视频类型数据库
    如果数据库文件不存在，则创建新的 SQLite 数据库
    如果数据库文件存在，则检查是否有必要更新表结构
    """
    create_table_sql = '''
        CREATE TABLE video_types (
            bvid TEXT,
            title TEXT,
            up_name TEXT,
            up_id INTEGER,
            pub_timestamp INTEGER,
            view INTEGER,
            like INTEGER,
            reply INTEGER,
            danmaku INTEGER,
            favorite INTEGER,
            coin INTEGER,
            share INTEGER,
            description TEXT,
            cover TEXT,
            duration INTEGER,
            tag TEXT,
            video_url TEXT,
            fetch_timestamp INTEGER,
            region_id INTEGER,
            type TEXT,
            PRIMARY KEY (bvid, type)  -- 联合主键，保证每个视频每种类型唯一
        )
    '''
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    if not os.path.exists(db_path):
        cursor.execute(create_table_sql)
        conn.commit()
    else:
        print(f"📦 数据库 `{db_path}` 已存在")
        # 检查表格是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='video_types';")
        if not cursor.fetchone():
            print("📦 创建视频类型信息表格 `video_types`")
            cursor.execute(create_table_sql)
            conn.commit()
        else:
            print("📦 表格 `video_types` 已存在")
            # 检查是否有必要更新表结构
            cursor.execute("PRAGMA table_info(video_types);")
            columns = [col[1] for col in cursor.fetchall()]
            required_columns = ['bvid', 'title', 'up_name', 'up_id',
                                'pub_timestamp', 'view', 'like', 'reply',
                                'danmaku', 'favorite', 'coin', 'share',
                                'description', 'cover', 'duration',
                                'tag', 'video_url', 'fetch_timestamp',
                                'region_id', 'type']
            for col in required_columns:
                if col not in columns:
                    print(f"⚠️ 列 `{col}` 不存在，可能需要手动更新表结构")

# 随机等待函数，防止请求过于频繁导致被封
def random_sleep(min_seconds=1, max_seconds=3):
    delay = random.uniform(min_seconds, max_seconds)
    print(f"⏳ 等待 {delay:.2f} 秒防封...")
    time.sleep(delay)

# 根据API获取视频数据并保存到数据库
# 获取分区视频最新投稿列表
def get_bilibili_newlist(
    rid,
    pn=1,
    ps=5
):
    """
    调用 Bilibili API 获取分区近期投稿列表

    参数说明：
    ----------
    rid : int
        分区 ID（非必须，但一般需要）。如 21 表示 "生活-日常"
    pn : int
        页码（默认 1）
    ps : int
        每页返回的视频数（默认 5，最大 50）

    返回值：
    ----------
    - pandas.DataFrame （默认）
    - dict 原始 JSON（如果 return_df=False）
    - None 请求失败或无数据
    """

    # =====================================
    # Step 1: 构造请求 URL 和参数
    # =====================================
    url = "https://api.bilibili.com/x/web-interface/newlist"
    params = {
        "rid": rid,    # 分区 ID（如果需要）
        "pn": pn,      # 当前页码
        "ps": ps,      # 每页返回的视频数
        "type": 0  # 类型参数（保留默认0）
    }

    # =====================================
    # Step 2: 伪装请求头（防止 412 拦截）
    # =====================================
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/114.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.bilibili.com",
        "Origin": "https://www.bilibili.com",
        "Accept": "application/json"
    }

    # =====================================
    # Step 3: 发送 GET 请求
    # =====================================
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()  # 抛出 HTTPError（非200响应）
        data = response.json()       # 解析 JSON 响应
    except requests.RequestException as e:
        print(f"❌ 请求失败: {e}")
        return None

    # =====================================
    # Step 4: 检查 API 返回状态
    # =====================================
    if data.get("code") != 0:
        print(f"⚠️ API 返回错误: code={data.get('code')} message={data.get('message')}")
        return None

    # 提取视频数据（同 dynamic/region 的 archives）
    archives = data.get("data", {}).get("archives", [])
    if not archives:
        print("📭 当前页无视频数据")
        return None

    # =====================================
    # Step 5: 格式化为 DataFrame
    # =====================================
    df = pd.DataFrame([{
        "BVID": v.get("bvid"),
        "标题": v.get("title"),
        "UP主": v.get("owner", {}).get("name"),
        "UP主ID": v.get("owner", {}).get("mid"),
        "发布时间戳": int(v.get("pubdate")),
        "播放数": v.get("stat", {}).get("view"),
        "点赞数": v.get("stat", {}).get("like"),
        "评论数": v.get("stat", {}).get("reply"),
        "弹幕数": v.get("stat", {}).get("danmaku"),
        "收藏数": v.get("stat", {}).get("favorite"),
        "投币数": v.get("stat", {}).get("coin"),
        "分享数": v.get("stat", {}).get("share"),
        "简介": v.get("desc"),
        "封面": v.get("pic"),
        "时长": v.get("duration"),
        "标签": v.get("tag"),
        "分区ID": rid,
        "视频链接": f"https://www.bilibili.com/video/{v.get('bvid')}",
        "获取时间戳": int(datetime.now().timestamp())
    } for v in archives])
    return df

# 获取视频的时间类型（1天、3天、1周、1月、3月、1年）
def get_video_type(pub_timestamp, fetch_timestamp):
    """
    根据发布时间和获取时间计算视频的时间类型（1天、3天、1周、1月、3月、1年）
    """

    # 获取各种类型的时间戳
    time_types = {
        '1_day': timedelta(days=1),
        '3_day': timedelta(days=3),
        '7_day': timedelta(days=7),
        '30_day': timedelta(days=30),
        '90_day': timedelta(days=90),
        '360_day': timedelta(days=360)
    }

    # 计算发布时间和获取时间的差值
    diff = fetch_timestamp - pub_timestamp
    best_type = None
    for t, delta in time_types.items():
        if abs(diff - delta.total_seconds()) <= delta.total_seconds() * 0.05:  # 允许5%的误差
            best_type = t
    return best_type

# 保存最新视频数据到数据库
# 注意：此函数假设数据库和表格已经存在，如果已经存在（BVID重复）则会覆盖
def save_video_to_db(video_data, db_path):
    """
    将获取的视频数据保存到数据库中
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for video in video_data:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO videos (
                    bvid, title, up_name, up_id, pub_timestamp, view, like, reply, danmaku,
                    favorite, coin, share, description, cover, duration, tag, video_url, fetch_timestamp, region_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                video['BVID'], video['标题'], video['UP主'], video['UP主ID'], video['发布时间戳'], 
                video['播放数'], video['点赞数'], video['评论数'], video['弹幕数'],
                video['收藏数'], video['投币数'], video['分享数'], video['简介'],
                video['封面'], video['时长'], video['标签'], video['视频链接'],
                video['获取时间戳'], video['分区ID']
            ))
        except Exception as e:
            print(f"❌ 插入数据时出错: {e}")
    conn.commit()
    conn.close()

# 保存视频类型到数据库
def save_video_type_to_db(video_data, db_path):
    """
    将视频的时间类型和其他所有信息保存到数据库中
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for video in video_data:
        try:
            video_type = get_video_type(video['发布时间戳'], video['获取时间戳'])

            if video_type:
                cursor.execute('''
                    INSERT OR REPLACE INTO video_types (
                        bvid, title, up_name, up_id, pub_timestamp, view, like, reply, danmaku, 
                        favorite, coin, share, description, cover, duration, tag, video_url, fetch_timestamp, region_id, type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    video['BVID'], video['标题'], video['UP主'], video['UP主ID'], video['发布时间戳'], 
                    video['播放数'], video['点赞数'], video['评论数'], video['弹幕数'],
                    video['收藏数'], video['投币数'], video['分享数'], video['简介'], 
                    video['封面'], video['时长'], video['标签'], video['视频链接'],
                    video['获取时间戳'], video['分区ID'], video_type
                ))
        except Exception as e:
            print(f"❌ 插入数据时出错: {e}")
    conn.commit()
    conn.close()

# 爬取视频数据并保存到 SQLite 数据库
# 1.视频最新细节信息
# 2.视频特定类型（如1天、3天、7天等）以及细节信息
def spider_and_save_video_data(region_id, video_details_db, video_details_with_type_db, page=1):
    """
    爬取指定分区的视频数据并保存到数据库中
    """

    # 爬取视频原始数据
    video_data = get_bilibili_newlist(rid = region_id, pn = page, ps = 50)
    
    # 解析视频数据并保存到 SQLite 数据库
    if video_data is not None and not video_data.empty:
        # 获取视频数据
        video_dict = video_data.to_dict(orient='records')
        # 将数据保存到视频数据库
        save_video_to_db(video_dict, video_details_db)
        # # 保存视频类型信息到新数据库
        save_video_type_to_db(video_dict, video_details_with_type_db)
        print(f"✅ 成功保存 {len(video_data)} 条数据到数据库，并计算了时间类型")
    else:
        print("📭 未获取到视频数据")
    
    return video_data

# 持续爬取 B 站视频详情数据并存入 SQLite 数据库
def continuously_spider_video_data(region_id, video_details_db, video_details_with_type_db, end_date = None, max_pages = 100, interval = 1):
    """
    持续循环获取视频数据并保存到数据库中
    """

    # 检查日期格式
    if end_date:
        try:
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            print("❌ 日期格式错误，请使用 YYYY-MM-DD 格式")
            return
    else:
        end_date = datetime.now() - timedelta(days=7)

    # 循环获取视频数据
    page = 1
    while True:
        print(f"📥 正在抓取第 {page} 页的视频数据...")
        try:
            video_data = spider_and_save_video_data(
                region_id = region_id,
                video_details_db = video_details_db,
                video_details_with_type_db = video_details_with_type_db,
                page = page
            )
        except Exception as e:
            print(f"❌ 抓取数据时出错: {e}")

        # 检查是否达到截止日期
        current_max_pub_timestamp = video_data['发布时间戳'].max() if not video_data.empty else 0
        if current_max_pub_timestamp < end_date.timestamp():
            print(f"📅 当前最大发布时间戳: {current_max_pub_timestamp} ({datetime.fromtimestamp(current_max_pub_timestamp)})")
            print(f"📅 截止日期: {end_date.timestamp()} ({end_date})")
            print(f"⏹ 已达到截止日期 {end_date.strftime('%Y-%m-%d')}，停止抓取")
            break

        # 检查是否达到最大页数限制
        if max_pages and page >= max_pages:
            print(f"⏹ 已达到最大页数限制 {max_pages}")
            break

        # 控制抓取间隔
        random_sleep(0.01, 0.1)
        print(f"⏳ 等待 {interval} 秒后抓取下一页...")
        time.sleep(interval)
        page += 1

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="爬取 B 站视频详情并存入 SQLite 数据库")
    parser.add_argument("region_id", type=int, help="B 站分区 ID")
    parser.add_argument("--video_details_db", type=str, default="video_details.db", help="视频详情 SQLite 数据库文件路径")
    parser.add_argument("--video_details_with_type_db", type=str, default="video_details_with_type.db", help="视频详情（带类型）SQLite 数据库文件路径")
    parser.add_argument("--end_date", type=str, default=None, help="截止日期（格式: YYYY-MM-DD），默认为7天前")
    parser.add_argument("--max_pages", type=int, default=100, help="最大爬取页数，默认为100")
    parser.add_argument("--interval", type=float, default=0.5, help="爬取间隔（秒），默认为半秒")
    args = parser.parse_args()
    
    # 初始化数据库连接
    init_video_db(args.video_details_db)
    init_video_type_db(args.video_details_with_type_db)

    # 开始不间断爬取视频数据
    continuously_spider_video_data(
        region_id=args.region_id,
        video_details_db=args.video_details_db,
        video_details_with_type_db=args.video_details_with_type_db,
        end_date=args.end_date,
        max_pages=args.max_pages,
        interval=args.interval
    )
