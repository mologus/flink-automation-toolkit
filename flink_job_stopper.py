#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Flink作业批量停止工具
可一键停止所有RUNNING状态的作业或指定作业，每次停止后间隔5秒
"""

import os
import sys
import json
import time
import http.client
import argparse
import base64
from datetime import datetime


class FlinkJobStopper:
    """
    
    
    Flink作业停止类
    提供批量停止作业的功能
    """
    
    def __init__(self, base_url, username="admin", password="admin", interval=5):
        """
        
        
        初始化停止工具
        
        @param base_url: API基础URL，例如 http://127.0.0.1:8081
        @param username: Basic Auth用户名
        @param password: Basic Auth密码
        @param interval: 停止作业间隔时间(秒)
        """
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.interval = interval
        
        # 解析主机名和协议
        if self.base_url.startswith('https://'):
            self.host = self.base_url[8:]  # 移除 'https://'
            self.use_https = True
        else:
            self.host = self.base_url[7:]  # 移除 'http://'
            self.use_https = False
        
        # 设置认证头
        auth_string = base64.b64encode(f"{username}:{password}".encode('utf-8')).decode('ascii')
        self.headers = {
            'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
            'Authorization': f'Basic {auth_string}',
            'Accept': '*/*',
            'Host': self.host,
            'Connection': 'keep-alive',
            'Content-Type': 'application/json'
        }
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  Flink作业停止工具初始化完成，API地址: {self.base_url}")
    
    def _make_request(self, path, method="GET", body=None):
        """
        
        
        发送HTTP请求到Flink API
        
        @param path: API路径
        @param method: HTTP方法 (GET, POST, etc.)
        @param body: 请求体，用于POST请求
        @return: 响应JSON对象
        """
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  请求API: {path} (方法: {method})")
        retries = 0
        max_retries = 3
        retry_delay = 2
        
        while retries <= max_retries:
            try:
                # 创建连接
                if self.use_https:
                    conn = http.client.HTTPSConnection(self.host, timeout=10)
                else:
                    conn = http.client.HTTPConnection(self.host, timeout=10)
                
                # 发送请求
                if body is not None:
                    body_str = json.dumps(body) if isinstance(body, dict) else body
                    conn.request(method, path, body=body_str, headers=self.headers)
                else:
                    conn.request(method, path, headers=self.headers)
                
                # 获取响应
                response = conn.getresponse()
                
                # 读取响应内容
                data = response.read().decode('utf-8')
                status = response.status
                
                # 解析响应
                if data and data.strip():
                    try:
                        result = json.loads(data)
                    except json.JSONDecodeError:
                        result = {"raw_response": data}
                else:
                    result = {"status": "success" if 200 <= status < 300 else "failure"}
                
                # 输出响应信息
                if 200 <= status < 300:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  请求成功，状态码: {status}")
                    return result
                else:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][ERROR]:  请求失败，状态码: {status}")
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][ERROR]:  响应内容: {data[:200]}...")
                    
                    if status == 401:
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][ERROR]:  认证失败，请检查用户名和密码")
                        raise Exception("认证失败")
                    raise Exception(f"请求失败，状态码: {status}")
                    
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][ERROR]:  请求错误: {str(e)}")
                
            finally:
                # 关闭连接
                if 'conn' in locals():
                    conn.close()
            
            # 重试逻辑
            retries += 1
            if retries <= max_retries:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  {retry_delay}秒后重试(第{retries}次)...")
                time.sleep(retry_delay)
            
        # 如果所有重试都失败，抛出异常
        raise Exception(f"请求失败，已重试{max_retries}次")
    
    def get_running_jobs(self):
        """
        
        
        获取所有RUNNING状态的作业
        
        @return: RUNNING状态的作业列表
        """
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  获取所有RUNNING状态作业...")
        
        # 获取作业概览
        result = self._make_request("/jobs/overview")
        
        if 'jobs' in result:
            # 筛选状态为RUNNING的作业
            running_jobs = [job for job in result['jobs'] if job.get('state') == 'RUNNING']
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  找到 {len(running_jobs)} 个RUNNING状态的作业(共 {len(result['jobs'])} 个作业)")
            
            if not running_jobs:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][WARN]:  未找到RUNNING状态的作业")
            
            return running_jobs
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][ERROR]:  API响应中没有jobs字段")
            return []
    
    def stop_job(self, job_id):
        """
        
        
        停止指定作业
        
        @param job_id: 作业ID(JID)
        @return: 停止请求结果
        """
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  停止作业: {job_id}")
        
        # 发送停止请求
        return self._make_request(f"/jobs/{job_id}/stop", method="POST", body="")
    
    def stop_all_jobs(self, job_ids=None, dry_run=False):
        """
        
        
        停止所有运行中的作业或指定作业
        
        @param job_ids: 要停止的作业ID列表，如果为None则停止所有运行中的作业
        @param dry_run: 是否只打印要执行的操作而不实际执行
        @return: 停止结果列表
        """
        # 获取要停止的作业
        if job_ids is None:
            running_jobs = self.get_running_jobs()
            job_ids = [(job['jid'], job.get('name', 'unknown')) for job in running_jobs]
        else:
            # 如果提供了作业ID列表，确保格式为 [(jid, name), ...]
            if job_ids and not isinstance(job_ids[0], tuple):
                job_ids = [(jid, 'unknown') for jid in job_ids]
            
        if not job_ids:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][WARN]:  没有作业需要停止")
            return []
        
        total_jobs = len(job_ids)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  准备停止 {total_jobs} 个作业")
        
        if dry_run:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  干运行模式，不会实际停止作业")
            for i, (jid, name) in enumerate(job_ids):
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  [{i+1}/{total_jobs}] 将停止作业: {jid} ({name})")
            return []
        
        results = []
        # 逐个停止作业
        for i, (jid, name) in enumerate(job_ids):
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  [{i+1}/{total_jobs}] 停止作业: {jid} ({name})")
            
            try:
                result = self.stop_job(jid)
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  成功停止作业: {jid}")
                results.append({
                    "job_id": jid,
                    "success": True,
                    "result": result
                })
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][ERROR]:  停止作业 {jid} 失败: {str(e)}")
                results.append({
                    "job_id": jid,
                    "success": False,
                    "error": str(e)
                })
            
            # 如果不是最后一个作业，等待一段时间
            if i < len(job_ids) - 1:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  等待 {self.interval} 秒后继续...")
                time.sleep(self.interval)
        
        # 统计结果
        success_count = sum(1 for r in results if r.get('success', False))
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  停止作业完成，成功: {success_count}/{total_jobs}")
        
        return results


def parse_arguments():
    """
    
    
    解析命令行参数
    
    @return: 解析后的参数对象
    """
    parser = argparse.ArgumentParser(description='Flink作业批量停止工具')
    
    parser.add_argument('--job-id', type=str, help='指定要停止的作业ID，多个ID用逗号分隔')
    parser.add_argument('--all', action='store_true', help='停止所有RUNNING状态的作业')
    parser.add_argument('--dry-run', action='store_true', help='不实际停止作业，只打印要执行的操作')
    parser.add_argument('--base-url', type=str, default='http://127.0.0.1:8081', help='Flink API基地址')
    parser.add_argument('--username', type=str, default='admin', help='API认证用户名')
    parser.add_argument('--password', type=str, default='admin', help='API认证密码')
    parser.add_argument('--interval', type=int, default=5, help='停止作业之间的间隔时间(秒)')
    
    return parser.parse_args()

def main():
    """
    
    
    主函数
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"===== Flink作业批量停止工具 (执行时间: {timestamp}) =====")
    
    # 解析命令行参数
    args = parse_arguments()
    
    # 验证参数
    if not args.all and not args.job_id:
        print("[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][ERROR]:  必须指定 --all 或 --job-id")
        return 1
    
    # 创建停止工具
    stopper = FlinkJobStopper(
        base_url=args.base_url,
        username=args.username,
        password=args.password,
        interval=args.interval
    )
    
    # 处理作业停止
    if args.job_id:
        # 停止指定的作业
        job_ids = [jid.strip() for jid in args.job_id.split(',') if jid.strip()]
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  将停止 {len(job_ids)} 个指定作业")
        results = stopper.stop_all_jobs(job_ids=job_ids, dry_run=args.dry_run)
    else:
        # 停止所有运行中的作业
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  将停止所有RUNNING状态的作业")
        results = stopper.stop_all_jobs(dry_run=args.dry_run)
    
    if results:
        success_count = sum(1 for r in results if r.get('success', False))
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  停止作业完成，成功: {success_count}/{len(results)}")
        return 0 if success_count == len(results) else 2
    else:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStopper][INFO]:  没有作业被处理")
        return 0

if __name__ == "__main__":
    sys.exit(main())

