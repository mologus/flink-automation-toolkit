#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试直接使用HTTP头部认证连接Flink API
"""

import http.client
import json
import base64
import requests

def test_http_client():
    """使用http.client进行连接测试"""
    print("=== 使用 http.client 测试连接 ===")
    conn = http.client.HTTPConnection("127.0.0.1", 8081)
    
    # 使用Basic认证
    auth_string = base64.b64encode(b"admin:admin").decode('ascii')
    headers = {
       'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
       'Authorization': f'Basic {auth_string}',
       'Accept': '*/*',
       'Host': '127.0.0.1:8081',
       'Connection': 'keep-alive'
    }
    
    # 尝试不同的API路径
    paths = ["/jars", "/jobs/overview", "/overview"]
    
    for path in paths:
        print(f"\n尝试路径: {path}")
        conn.request("GET", path, "", headers)
        res = conn.getresponse()
        data = res.read()
        
        print(f"状态码: {res.status}")
        
        if res.status == 200:
            try:
                json_data = json.loads(data.decode("utf-8"))
                print(f"响应JSON: {json.dumps(json_data, indent=2)[:500]}...")
            except json.JSONDecodeError:
                print(f"响应文本: {data.decode('utf-8')[:500]}...")
        else:
            print(f"响应内容: {data.decode('utf-8')[:500]}...")

def test_requests():
    """使用requests库进行连接测试"""
    print("\n=== 使用 requests 测试连接 ===")
    
    # 方式1：使用auth参数
    print("\n方式1: 使用auth参数")
    try:
        response = requests.get(
            "http://127.0.0.1:8081/jobs/overview",
            auth=("admin", "admin"),
            timeout=10
        )
        print(f"状态码: {response.status_code}")
        if response.status_code == 200:
            print(f"响应JSON: {json.dumps(response.json(), indent=2)[:500]}...")
        else:
            print(f"响应文本: {response.text[:500]}...")
    except Exception as e:
        print(f"错误: {str(e)}")
    
    # 方式2：直接使用headers参数
    print("\n方式2: 使用headers参数")
    try:
        auth_string = base64.b64encode(b"admin:admin").decode('ascii')
        headers = {
            'Authorization': f'Basic {auth_string}',
            'Accept': '*/*',
            'User-Agent': 'Apifox/1.0.0 (https://apifox.com)'
        }
        
        response = requests.get(
            "http://127.0.0.1:8081",
            headers=headers,
            timeout=10
        )
        print(f"状态码: {response.status_code}")
        if response.status_code == 200:
            print(f"响应JSON: {json.dumps(response.json(), indent=2)[:500]}...")
        else:
            print(f"响应文本: {response.text[:500]}...")
    except Exception as e:
        print(f"错误: {str(e)}")

if __name__ == "__main__":
    test_http_client()
    test_requests()
