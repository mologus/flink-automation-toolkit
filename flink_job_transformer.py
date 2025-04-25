#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Flink作业转换工具
从Flink API获取作业信息并转换为指定格式
"""

import os
import json
import base64
import argparse
import requests
import http.client
import sys
from urllib.parse import urlparse


class FlinkJobTransformer:
    """
    
    
    Flink作业转换工具类
    负责从Flink API获取作业信息并转换为指定格式
    """
    
    def __init__(self, base_url, username="flink", password="flink-!@xxx2021"):
        """
        
        
        初始化转换工具
        
        @param base_url: API基础URL，例如 https://flink.xxx.com
        @param username: Basic Auth用户名
        @param password: Basic Auth密码
        """
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        
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
            'Connection': 'keep-alive'
        }
        
        print(f"[INFO] 初始化转换工具，API地址: {self.base_url}")
    
    def get_finished_jobs(self):
        """
        
        
        获取状态为FINISHED的作业ID列表
        
        @return: 作业ID列表
        """
        print(f"[INFO] 获取状态为FINISHED的作业列表...")
        
        try:
            # 创建连接
            if self.use_https:
                conn = http.client.HTTPSConnection(self.host, timeout=10)
            else:
                conn = http.client.HTTPConnection(self.host, timeout=10)
            
            # 发送请求
            conn.request("GET", "/jobs/overview", headers=self.headers)
            
            # 获取响应
            response = conn.getresponse()
            data = response.read().decode('utf-8')
            
            if response.status == 200:
                result = json.loads(data)
                if 'jobs' in result:
                    # 筛选状态为FINISHED的作业
                    finished_jobs = [job for job in result['jobs'] if job.get('state') == 'FINISHED']
                    print(f"[INFO] 找到 {len(finished_jobs)} 个状态为FINISHED的作业(共 {len(result['jobs'])} 个作业)")
                    return finished_jobs
                else:
                    print(f"[ERROR] API响应中没有jobs字段")
                    return []
            else:
                print(f"[ERROR] 请求失败，状态码: {response.status}")
                print(f"[ERROR] 响应内容: {data[:200]}...")
                return []
                
        except Exception as e:
            print(f"[ERROR] 获取作业列表时出错: {str(e)}")
            return []
        finally:
            if 'conn' in locals():
                conn.close()
    
    def get_job_checkpoints(self, job_id):
        """
        
        
        获取作业的checkpoint信息
        
        @param job_id: 作业ID
        @return: checkpoint信息对象
        """
        print(f"[INFO] 获取作业 {job_id} 的checkpoint信息...")
        
        try:
            # 创建连接
            if self.use_https:
                conn = http.client.HTTPSConnection(self.host, timeout=10)
            else:
                conn = http.client.HTTPConnection(self.host, timeout=10)
            
            # 发送请求
            conn.request("GET", f"/jobs/{job_id}/checkpoints", headers=self.headers)
            
            # 获取响应
            response = conn.getresponse()
            data = response.read().decode('utf-8')
            
            if response.status == 200:
                result = json.loads(data)
                
                # 检查是否有savepoint信息
                if 'latest' in result and 'savepoint' in result['latest'] and 'external_path' in result['latest']['savepoint']:
                    savepoint_path = result['latest']['savepoint']['external_path']
                    print(f"[INFO] 成功获取savepoint路径: {savepoint_path}")
                    return result
                else:
                    print(f"[WARN] 未找到savepoint信息")
                    return result
            else:
                print(f"[ERROR] 请求失败，状态码: {response.status}")
                print(f"[ERROR] 响应内容: {data[:200]}...")
                return None
                
        except Exception as e:
            print(f"[ERROR] 获取checkpoint信息时出错: {str(e)}")
            return None
        finally:
            if 'conn' in locals():
                conn.close()
    
    def transform_job_config(self, job_config, savepoint_path=None):
        """
        
        
        将作业配置转换为指定格式
        
        @param job_config: 原始作业配置
        @param savepoint_path: savepoint路径，如果为None则不包含
        @return: 转换后的配置
        """
        print(f"[INFO] 转换作业配置...")
        
        # 提取需要的原始配置部分
        config_data = {}
        if "marketing.ddl" in job_config:
            config_data["marketing.ddl"] = job_config["marketing.ddl"]
        if "marketing.sql" in job_config:
            config_data["marketing.sql"] = job_config["marketing.sql"]
        if "pipeline.name" in job_config:
            config_data["pipeline.name"] = job_config["pipeline.name"]
        
        # Base64编码
        program_args = base64.b64encode(json.dumps(config_data, ensure_ascii=False).encode('utf-8')).decode('utf-8')
        
        # 构建新格式
        transformed_config = {
            "entryClass": "com.quick.marketing.MarketingSQLTask",
            "parallelism": None,
            "programArgs": program_args
        }
        
        # 添加savepoint路径（如果有）
        if savepoint_path:
            transformed_config["savepointPath"] = savepoint_path
        
        return transformed_config
    
    def process_all(self, input_file, output_file=None):
        """
        
        
        处理所有作业
        
        @param input_file: 输入文件，包含原始作业配置
        @param output_file: 输出文件，如果为None则使用input_file加上.transformed后缀
        @return: 成功处理的作业数量
        """
        if output_file is None:
            output_file = f"{os.path.splitext(input_file)[0]}.transformed.json"
        
        print(f"[INFO] 从 {input_file} 读取原始配置...")
        
        try:
            # 读取原始配置
            with open(input_file, 'r', encoding='utf-8') as f:
                original_configs = json.load(f)
            
            print(f"[INFO] 读取到 {len(original_configs)} 个作业配置")
            
            # 获取FINISHED状态的作业
            finished_jobs = self.get_finished_jobs()
            finished_job_ids = [job.get('jid') for job in finished_jobs]
            
            # 初始化结果
            transformed_configs = {}
            
            # 处理每个原始配置
            for job_id, config in original_configs.items():
                print(f"[INFO] 处理作业 {job_id}...")
                
                # 检查该作业是否处于FINISHED状态
                if job_id in finished_job_ids:
                    print(f"[INFO] 作业 {job_id} 处于FINISHED状态，获取checkpoint信息...")
                    
                    # 获取checkpoint信息
                    checkpoints = self.get_job_checkpoints(job_id)
                    savepoint_path = None
                    
                    if checkpoints and 'latest' in checkpoints and 'savepoint' in checkpoints['latest']:
                        savepoint_path = checkpoints['latest']['savepoint'].get('external_path')
                        print(f"[INFO] 获取到savepoint路径: {savepoint_path}")
                    
                    # 转换配置
                    transformed_config = self.transform_job_config(config, savepoint_path)
                    transformed_configs[job_id] = transformed_config
                    print(f"[INFO] 作业 {job_id} 转换完成")
                else:
                    print(f"[WARN] 作业 {job_id} 不是FINISHED状态，跳过")
            
            # 保存结果
            print(f"[INFO] 将转换后的配置保存到 {output_file}...")
            os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(transformed_configs, f, indent=2, ensure_ascii=False)
            
            print(f"[INFO] 成功处理 {len(transformed_configs)} 个作业配置")
            return len(transformed_configs)
            
        except Exception as e:
            print(f"[ERROR] 处理过程中出错: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return 0

def parse_arguments():
    """
    
    
    解析命令行参数
    
    @return: 解析后的参数对象
    """
    parser = argparse.ArgumentParser(description='Flink作业配置转换工具')
    
    parser.add_argument('--input', type=str, required=True, help='输入文件路径，包含原始作业配置')
    parser.add_argument('--output', type=str, help='输出文件路径')
    parser.add_argument('--base-url', type=str, default='http://127.0.0.1:8081', help='Flink API基地址')
    parser.add_argument('--username', type=str, default='flink', help='API认证用户名')
    parser.add_argument('--password', type=str, default='flink-!@xxx2021', help='API认证密码')
    
    return parser.parse_args()

def main():
    """
    
    
    主函数
    """
    print("=== Flink作业转换工具 ===")
    
    # 解析命令行参数
    args = parse_arguments()
    
    # 创建转换器
    transformer = FlinkJobTransformer(
        base_url=args.base_url,
        username=args.username,
        password=args.password
    )
    
    # 处理所有作业
    success_count = transformer.process_all(
        input_file=args.input,
        output_file=args.output
    )
    
    print(f"=== 转换完成，共成功处理 {success_count} 个作业配置 ===")
    
    return 0 if success_count > 0 else 1

if __name__ == "__main__":
    sys.exit(main())

