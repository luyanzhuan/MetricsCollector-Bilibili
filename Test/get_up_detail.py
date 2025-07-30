#!/usr/bin/env python
# coding=utf-8
'''
Author       : luyz
Date         : 2025-07-29 16:18:48
LastEditors  : luyz
LastEditTime : 2025-07-29 16:24:47
Description  : 
Copyright (c) 2025 by luyz && luyz@aptbiotech.com, All Rights Reserved. 
'''
import requests  # 导入requests库用于发送HTTP请求
import pandas as pd  # 导入pandas库用于数据结构(DataFrame)处理

def get_followers(mid_list):
    """
    批量获取 Bilibili 用户的粉丝数
    """
    results = []

    # 加强型请求头：绕过 B 站 412 限制
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Referer": "https://space.bilibili.com/",
        "Origin": "https://space.bilibili.com",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9"
    }

    for mid in mid_list:
        try:
            url = f"https://api.bilibili.com/x/relation/stat?vmid={mid}"
            response = requests.get(url, headers=headers, timeout=5)

            if response.status_code == 200 and response.text.strip():
                data = response.json()
                print(data)
                if data.get('code') == 0:
                    follower_count = data.get('data', {}).get('follower', None)
                else:
                    print(f"⚠️ mid {mid} 查询失败，返回码：{data.get('code')}")
                    follower_count = None
            else:
                print(f"⚠️ mid {mid} 返回状态码：{response.status_code}，可能被反爬")
                follower_count = None

        except Exception as e:
            print(f"❌ 查询用户 {mid} 时异常: {e}")
            follower_count = None

        results.append({'mid': mid, 'follower': follower_count})

    return pd.DataFrame(results)

if __name__ == "__main__":
    # 测试用例：查询用户ID为1, 172, 5566的粉丝数
    mids = [326427334, 94742590, 400365390]
    df = get_followers(mids)
    print(df)
    # 输出示例:
    #    mid  follower
    # 0    1     10000
    # 1  172      5000
    # 2 5566      NaN   # 如果查询失败则显示NaN