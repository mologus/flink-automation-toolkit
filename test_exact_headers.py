#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
精确模拟Apifox请求的测试脚本
"""

import requests
import json
import http.client
import sys

def test_exact_headers():
    """使用完全相同的请求头进行测试"""
    print("=== 使用完全相同的请求头进行测试 ===")

    # 使用http.client
    print("\n方法1: 使用http.client")
    conn = http.client.HTTPConnection("127.0.0.1", 8081)
    
    # 使用Basic认证
    import base64
    auth_string = base64.b64encode(b"admin:admin").decode('ascii')
    headers = {
       'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
       'Authorization': f'Basic {auth_string}',
       'Accept': '*/*',
       'Host': '127.0.0.1:8081',
       'Connection': 'keep-alive'
    }
    
    paths = ["/jobs/overview", "/overview", "/jobs", "/"]
    
    for path in paths:
        try:
            print(f"\n尝试路径: {path}")
            conn.request("GET", path, None, headers)
            response = conn.getresponse()
            data = response.read()
            
            print(f"状态码: {response.status}")
            print(f"响应头: {response.getheaders()}")
            
            if response.status == 200:
                try:
                    json_data = json.loads(data.decode("utf-8"))
                    print(f"响应数据: {json.dumps(json_data, indent=2)[:500]}...")
                except:
                    print(f"响应内容: {data.decode('utf-8')[:500]}...")
            else:
                print(f"错误响应: {data.decode('utf-8')[:500]}...")
        except Exception as e:
            print(f"请求失败: {str(e)}")
    
    # 使用requests
    print("\n方法2: 使用requests")
    for path in paths:
        try:
            print(f"\n尝试路径: {path}")
            url = f"http://127.0.0.1:8081{path}"
            
            # 更新headers使用正确的认证信息
            req_headers = headers.copy()
            req_headers['Host'] = '127.0.0.1:8081'  # 确保Host头是正确的
            
            response = requests.get(url, headers=req_headers)
            
            print(f"状态码: {response.status_code}")
            print(f"响应头: {dict(response.headers)}")
            
            if response.status_code == 200:
                try:
                    json_data = response.json()
                    print(f"响应数据: {json.dumps(json_data, indent=2)[:500]}...")
                except:
                    print(f"响应内容: {response.text[:500]}...")
            else:
                print(f"错误响应: {response.text[:500]}...")
        except Exception as e:
            print(f"请求失败: {str(e)}")
            
def main():
    """主函数"""
    try:
        test_exact_headers()
        return 0
    except Exception as e:
        print(f"测试过程中出错: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())
