#!/usr/bin/env python
# coding=utf-8
'''
FilePath     : /luyz/project/CustomizedAnalysis/CustomizedAnalysis.12.20250726_YASbilibili/Test/test_get_up_followers.py
Author       : luyz
Date         : 2025-07-30 09:19:29
LastEditors  : luyz
LastEditTime : 2025-07-30 09:20:46
Description  : 
Copyright (c) 2025 by luyz && luyz@aptbiotech.com, All Rights Reserved. 
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

# 定义 User-Agent 列表（可扩充）
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/104.0.0.0 Safari/537.36"
]

# 获取 UP 主的粉丝数
def get_up_followers(up_id):
    # Step 1: 请求基本参数
    url = "https://api.bilibili.com/x/relation/stat"
    params = {"vmid": up_id}

    # Step 2: 重试设置（带指数退避）
    max_retries = 3
    wait_time = 1
    max_wait = 10
    followers_count = None

    for attempt in range(1, max_retries + 1):
        # Step 3: 伪装请求头
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Referer": "https://www.bilibili.com",
            "Origin": "https://www.bilibili.com",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            break  # 请求成功，跳出重试循环
        except requests.exceptions.Timeout:
            if attempt < max_retries:
                sleep_sec = min(wait_time, max_wait)
                print(f"⚠️ 请求超时，第 {attempt} 次，等待 {sleep_sec}s 后重试...")
                time.sleep(sleep_sec)
                wait_time *= 2  # 指数退避
            else:
                print("❌ 请求超时，已放弃")
                return None
        except requests.RequestException as e:
            print(f"❌ 请求失败: {e}")
            return None

    # Step 5: 响应校验
    if data is None or data.get("code") != 0:
        err_code = data.get("code") if data else "None"
        err_msg = data.get("message") if data else "No response"
        print(f"⚠️ API 返回错误: code={err_code} message={err_msg}")
        return None

    # Step 6: 提取视频数据并转换为 DataFrame
    if 'data' in data and 'follower' in data['data']:
        followers_count = data['data']['follower']
    else:
        print("📭 未获取到粉丝数")
        return None

    return followers_count

# 主函数
if __name__ == "__main__":
    uid = 3546869617657989  # 示例 UP 主 ID
    followers = get_up_followers(uid)
    print(f"UP 主 {uid} 的粉丝数: {followers if followers is not None else '查询失败'}")