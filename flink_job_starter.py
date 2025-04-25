#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Flink作业批量启动工具
可一键启动所有SQL作业，使用配置文件中的参数启动作业
"""

import os
import sys
import json
import time
import http.client
import argparse
import base64
from datetime import datetime


class FlinkJobStarter:
    """
    
    
    Flink作业启动类
    提供批量启动SQL作业的功能
    """
    
    def __init__(self, base_url, username="admin", password="admin", 
                 common_jar_id="example-jar-id_common-flink-job.jar", 
                 interval=10):
        """
        
        
        初始化启动工具
        
        @param base_url: API基础URL，例如 http://127.0.0.1:8081
        @param username: Basic Auth用户名
        @param password: Basic Auth密码
        @param common_jar_id: 公共JAR文件ID
        @param interval: 启动作业间隔时间(秒)
        """
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.common_jar_id = common_jar_id
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
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  Flink作业启动工具初始化完成，API地址: {self.base_url}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  使用JAR ID: {self.common_jar_id}")
    
    def _make_request(self, path, method="GET", body=None):
        """
        
        
        发送HTTP请求到Flink API
        
        @param path: API路径
        @param method: HTTP方法 (GET, POST, etc.)
        @param body: 请求体，用于POST请求
        @return: 响应JSON对象
        """
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  请求API: {path} (方法: {method})")
        retries = 0
        max_retries = 3
        retry_delay = 2
        
        while retries <= max_retries:
            try:
                # 创建连接
                if self.use_https:
                    conn = http.client.HTTPSConnection(self.host, timeout=30)
                else:
                    conn = http.client.HTTPConnection(self.host, timeout=30)
                
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
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  请求成功，状态码: {status}")
                    return result
                else:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][ERROR]:  请求失败，状态码: {status}")
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][ERROR]:  响应内容: {data[:200]}...")
                    
                    if status == 401:
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][ERROR]:  认证失败，请检查用户名和密码")
                        raise Exception("认证失败")
                    raise Exception(f"请求失败，状态码: {status}")
                    
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][ERROR]:  请求错误: {str(e)}")
                
            finally:
                # 关闭连接
                if 'conn' in locals():
                    conn.close()
            
            # 重试逻辑
            retries += 1
            if retries <= max_retries:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  {retry_delay}秒后重试(第{retries}次)...")
                time.sleep(retry_delay)
            
        # 如果所有重试都失败，抛出异常
        raise Exception(f"请求失败，已重试{max_retries}次")
    
    def start_job(self, job_config):
        """
        
        
        启动单个作业
        
        @param job_config: 作业配置
        @return: 启动请求结果
        """
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  启动作业，使用JAR: {self.common_jar_id}")
        
        # 发送启动请求
        return self._make_request(f"/jars/{self.common_jar_id}/run", method="POST", body=job_config)
    
    def load_job_configs(self, config_file):
        """
        
        
        加载作业配置
        
        @param config_file: 配置文件路径
        @return: 作业配置列表
        """
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  从文件加载作业配置: {config_file}")
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                configs = json.load(f)
                
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  成功加载 {len(configs)} 个作业配置")
            return configs
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][ERROR]:  加载配置文件失败: {str(e)}")
            return {}
    
    def start_all_jobs(self, configs, dry_run=False):
        """
        
        
        启动所有作业
        
        @param configs: 作业配置字典
        @param dry_run: 是否只打印要执行的操作而不实际执行
        @return: 启动结果列表
        """
        if not configs:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][WARN]:  没有作业需要启动")
            return []
        
        job_ids = list(configs.keys())
        total_jobs = len(job_ids)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  准备启动 {total_jobs} 个作业")
        
        if dry_run:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  干运行模式，不会实际启动作业")
            for i, job_id in enumerate(job_ids):
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  [{i+1}/{total_jobs}] 将启动作业: {job_id}")
            return []
        
        results = []
        # 逐个启动作业
        for i, job_id in enumerate(job_ids):
            job_config = configs[job_id]
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  [{i+1}/{total_jobs}] 启动作业: {job_id}")
            
            try:
                result = self.start_job(job_config)
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  成功启动作业: {job_id}")
                
                # 如果响应中有jobid，输出提示
                if isinstance(result, dict) and 'jobid' in result:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  新的作业ID: {result['jobid']}")
                
                results.append({
                    "job_id": job_id,
                    "success": True,
                    "result": result
                })
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][ERROR]:  启动作业 {job_id} 失败: {str(e)}")
                results.append({
                    "job_id": job_id,
                    "success": False,
                    "error": str(e)
                })
            
            # 如果不是最后一个作业，等待一段时间
            if i < total_jobs - 1:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  等待 {self.interval} 秒后继续...")
                time.sleep(self.interval)
        
        # 统计结果
        success_count = sum(1 for r in results if r.get('success', False))
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  启动作业完成，成功: {success_count}/{total_jobs}")
        
        return results


def parse_arguments():
    """
    
    
    解析命令行参数
    
    @return: 解析后的参数对象
    """
    parser = argparse.ArgumentParser(description='Flink作业批量启动工具')
    
    parser.add_argument('--config-file', type=str, default='output/processed_jobs.json', help='作业配置文件')
    parser.add_argument('--job-id', type=str, help='指定要启动的作业ID，多个ID用逗号分隔')
    parser.add_argument('--all', action='store_true', help='启动配置文件中的所有作业')
    parser.add_argument('--dry-run', action='store_true', help='不实际启动作业，只打印要执行的操作')
    parser.add_argument('--base-url', type=str, default='http://127.0.0.1:8081', help='Flink API基地址')
    parser.add_argument('--username', type=str, default='admin', help='API认证用户名')
    parser.add_argument('--password', type=str, default='admin', help='API认证密码')
    parser.add_argument('--jar-id', type=str, default='example-jar-id_common-flink-job.jar', help='公共JAR文件ID')
    parser.add_argument('--interval', type=int, default=10, help='启动作业之间的间隔时间(秒)')
    
    return parser.parse_args()

def main():
    """
    
    
    主函数
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"===== Flink作业批量启动工具 (执行时间: {timestamp}) =====")
    
    # 解析命令行参数
    args = parse_arguments()
    
    # 验证参数
    if not args.all and not args.job_id:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][ERROR]:  必须指定 --all 或 --job-id")
        return 1
    
    # 创建启动工具
    starter = FlinkJobStarter(
        base_url=args.base_url,
        username=args.username,
        password=args.password,
        common_jar_id=args.jar_id,
        interval=args.interval
    )
    
    # 加载配置
    all_configs = starter.load_job_configs(args.config_file)
    if not all_configs:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][ERROR]:  配置文件为空或无法加载")
        return 1
    
    # 处理作业启动
    if args.job_id:
        # 启动指定的作业
        job_ids = [jid.strip() for jid in args.job_id.split(',') if jid.strip()]
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  将启动 {len(job_ids)} 个指定作业")
        
        # 筛选指定的作业配置
        selected_configs = {k: all_configs[k] for k in job_ids if k in all_configs}
        
        # 检查是否有未找到的作业
        missing = [jid for jid in job_ids if jid not in all_configs]
        if missing:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][WARN]:  以下作业在配置文件中未找到: {', '.join(missing)}")
        
        results = starter.start_all_jobs(selected_configs, dry_run=args.dry_run)
    else:
        # 启动所有作业
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  将启动配置文件中的所有作业")
        results = starter.start_all_jobs(all_configs, dry_run=args.dry_run)
    
    if results:
        success_count = sum(1 for r in results if r.get('success', False))
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  启动作业完成，成功: {success_count}/{len(results)}")
        return 0 if success_count == len(results) else 2
    else:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobStarter][INFO]:  没有作业被处理")
        return 0

if __name__ == "__main__":
    sys.exit(main())

