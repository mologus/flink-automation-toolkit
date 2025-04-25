#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Flink作业处理工具
从Flink API获取所有数据，完成端到端处理流程
"""

import os
import sys
import json
import base64
import argparse
import http.client
import time
import glob
from datetime import datetime
from src.etl_mapping import ETLMapping


class FlinkJobProcessor:
    """
    
    
    Flink作业处理类
    实现完整的端到端处理流程
    """
    
    def __init__(self, base_url, username="admin", password="admin"):
        """
        
        
        初始化处理工具
        
        @param base_url: API基础URL，例如 http://127.0.0.1:8081
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
        import base64
        auth_string = base64.b64encode(f"{username}:{password}".encode('utf-8')).decode('ascii')
        self.headers = {
            'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
            'Authorization': f'Basic {auth_string}',
            'Accept': '*/*',
            'Host': self.host,
            'Connection': 'keep-alive'
        }
        
        print(f"[INFO] Flink作业处理工具初始化完成，API地址: {self.base_url}")
    
    def _make_request(self, path):
        """
        
        
        发送HTTP请求到Flink API
        
        @param path: API路径
        @return: 响应JSON对象
        """
        print(f"[INFO] 请求API: {path}")
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
                conn.request("GET", path, headers=self.headers)
                
                # 获取响应
                response = conn.getresponse()
                data = response.read().decode('utf-8')
                
                # 输出响应信息
                status = response.status
                
                if status == 200:
                    # 解析JSON响应
                    result = json.loads(data)
                    print(f"[INFO] 请求成功，状态码: {status}")
                    return result
                else:
                    print(f"[ERROR] 请求失败，状态码: {status}")
                    print(f"[ERROR] 响应内容: {data[:200]}...")
                    
                    if status == 401:
                        print(f"[ERROR] 认证失败，请检查用户名和密码")
                        raise Exception("认证失败")
                    raise Exception(f"请求失败，状态码: {status}")
                    
            except Exception as e:
                print(f"[ERROR] 请求错误: {str(e)}")
                
            finally:
                # 关闭连接
                if 'conn' in locals():
                    conn.close()
            
            # 重试逻辑
            retries += 1
            if retries <= max_retries:
                print(f"[INFO] {retry_delay}秒后重试(第{retries}次)...")
                time.sleep(retry_delay)
            
        # 如果所有重试都失败，抛出异常
        raise Exception(f"请求失败，已重试{max_retries}次")
    
    def save_raw_jobs_data(self, jobs_data):
        """
        
        
        保存原始作业数据到文件，带时间戳
        
        @param jobs_data: 原始作业数据
        @return: 保存的文件路径
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_data_dir = "output/raw_data"
        os.makedirs(raw_data_dir, exist_ok=True)
        
        filename = f"raw_jobs_{timestamp}.json"
        filepath = os.path.join(raw_data_dir, filename)
        
        # 构建数据结构
        data = {
            "timestamp": timestamp,
            "api_url": self.base_url,
            "jobs": jobs_data
        }
        
        # 保存到文件
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  原始作业数据已保存到: {filepath}")
        return filepath
    
    def list_available_replays(self):
        """
        
        
        列出所有可用的重放数据集
        
        @return: 可用的重放数据集列表，每个元素包含时间戳和文件路径
        """
        raw_data_dir = "output/raw_data"
        if not os.path.exists(raw_data_dir):
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  原始数据目录不存在: {raw_data_dir}")
            return []
        
        # 查找所有raw_jobs_*.json文件
        pattern = os.path.join(raw_data_dir, "raw_jobs_*.json")
        files = glob.glob(pattern)
        
        # 提取时间戳并排序
        replays = []
        for file in files:
            try:
                filename = os.path.basename(file)
                timestamp = filename[9:-5]  # 提取raw_jobs_与.json之间的部分
                
                # 验证这是有效的时间戳格式
                datetime.strptime(timestamp, "%Y%m%d_%H%M%S")
                
                # 读取文件，获取作业数量
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    jobs_count = len(data.get('jobs', []))
                    api_url = data.get('api_url', 'unknown')
                
                replays.append({
                    "timestamp": timestamp,
                    "filepath": file,
                    "jobs_count": jobs_count,
                    "api_url": api_url
                })
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][WARN]:  解析重放文件失败: {file}, 原因: {str(e)}")
                continue
        
        # 按时间戳倒序排序（最新的在前面）
        replays.sort(key=lambda x: x['timestamp'], reverse=True)
        
        if not replays:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  未找到可用的重放数据集")
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  找到 {len(replays)} 个可用的重放数据集")
            for i, replay in enumerate(replays):
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  [{i+1}] 时间戳: {replay['timestamp']}, 作业数: {replay['jobs_count']}, API: {replay['api_url']}")
                
        return replays
    
    def load_raw_jobs_data(self, timestamp=None):
        """
        
        
        加载特定时间戳或最新的原始作业数据
        
        @param timestamp: 指定的时间戳，如果为None则加载最新的
        @return: 加载的原始作业数据
        """
        if timestamp:
            # 加载指定时间戳的数据
            filepath = os.path.join("output/raw_data", f"raw_jobs_{timestamp}.json")
            if not os.path.exists(filepath):
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][ERROR]:  指定的重放数据不存在: {filepath}")
                return None
            
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  加载指定时间戳的重放数据: {timestamp}")
        else:
            # 获取最新的数据
            replays = self.list_available_replays()
            if not replays:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][ERROR]:  没有可用的重放数据")
                return None
            
            filepath = replays[0]['filepath']
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  加载最新的重放数据: {os.path.basename(filepath)}")
        
        # 加载数据
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            jobs_data = data.get('jobs', [])
            timestamp = data.get('timestamp', 'unknown')
            api_url = data.get('api_url', 'unknown')
            
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  成功加载重放数据，时间戳: {timestamp}, API: {api_url}, 作业数: {len(jobs_data)}")
            return jobs_data
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][ERROR]:  加载重放数据失败: {str(e)}")
            return None
    
    def get_finished_jobs(self, save_raw_data=True):
        """
        
        
        获取所有FINISHED状态的作业，并选择性保存原始数据
        
        @param save_raw_data: 是否保存原始数据
        @return: FINISHED状态的作业列表
        """
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  获取所有FINISHED状态作业...")
        
        # 获取作业概览
        result = self._make_request("/jobs/overview")
        
        if 'jobs' in result:
            # 如果开启了保存原始数据，则先保存完整的原始数据
            if save_raw_data:
                self.save_raw_jobs_data(result['jobs'])
            
            # 筛选状态为FINISHED的作业
            finished_jobs = [job for job in result['jobs'] if job.get('state') == 'FINISHED']
            print(f"[INFO] 找到 {len(finished_jobs)} 个FINISHED状态的作业(共 {len(result['jobs'])} 个作业)")
            
            if not finished_jobs:
                print(f"[WARN] 未找到FINISHED状态的作业")
            
            return finished_jobs
        else:
            print(f"[ERROR] API响应中没有jobs字段")
            return []
    
    def get_job_config(self, job_id):
        """
        
        
        获取作业配置
        
        @param job_id: 作业ID(JID)
        @return: 作业配置
        """
        print(f"[INFO] 获取作业配置: {job_id}")
        
        return self._make_request(f"/jobs/{job_id}/config")
    
    def get_job_checkpoints(self, job_id):
        """
        
        
        获取作业的checkpoint信息
        
        @param job_id: 作业ID(JID)
        @return: checkpoint信息
        """
        print(f"[INFO] 获取作业checkpoint信息: {job_id}")
        
        return self._make_request(f"/jobs/{job_id}/checkpoints")
    
    def _get_ttl_by_pipeline_name(self, pipeline_name):
        """
        
        
        根据pipeline.name获取对应的TTL值
        
        @param pipeline_name: 作业名称
        @return: TTL值，如果没有匹配则返回None
        """
        # TTL映射关系
        ttl_mapping = {
            'daily_task_task': '2',
            'new_user_task': '2',
            'weekly_gold_task': '8',
            'Live_task': '32',
            'weekly_bean_task': '8',
            'agent_reward_task': '32',
            'player_20240930_task': '8',
            'player_20241104_task': '8',
            'honorAgent_20250301_task': '32',
            'player_20250303_task': '8',
            'honorAgent_20250401_task': '32',
            'player_20250331_task': '8',
            'gameKing_20250401_task': '32',
            'honorAgent_20250501_task': '32',
            'player_20250505_task': '8',
            'gameKing_20250501_task': '32'
        }
        
        return ttl_mapping.get(pipeline_name)
    
    def extract_marketing_config(self, job_config):
        """
        
        
        从作业配置中提取marketing配置
        
        @param job_config: 作业配置
        @return: 提取出的marketing配置
        """
        print(f"[INFO] 提取作业marketing配置")
        
        result = {}
        
        # 从execution-config.user-config中提取
        if 'execution-config' in job_config and 'user-config' in job_config['execution-config']:
            user_config = job_config['execution-config']['user-config']
            
            # 提取marketing.ddl
            if 'marketing.ddl' in user_config:
                try:
                    # 可能是JSON字符串，需要解析
                    ddl_value = user_config['marketing.ddl']
                    if isinstance(ddl_value, str) and (ddl_value.startswith('[') or ddl_value.startswith('"')):
                        result['marketing.ddl'] = json.loads(ddl_value)
                    else:
                        result['marketing.ddl'] = ddl_value
                    print(f"[INFO] 成功提取marketing.ddl，包含 {len(result['marketing.ddl'])} 条语句")
                except Exception as e:
                    print(f"[WARN] 解析marketing.ddl失败: {str(e)}")
                    result['marketing.ddl'] = user_config['marketing.ddl']
            
            # 提取marketing.sql
            if 'marketing.sql' in user_config:
                try:
                    # 可能是JSON字符串，需要解析
                    sql_value = user_config['marketing.sql']
                    if isinstance(sql_value, str) and (sql_value.startswith('[') or sql_value.startswith('"')):
                        result['marketing.sql'] = json.loads(sql_value)
                    else:
                        result['marketing.sql'] = sql_value
                    print(f"[INFO] 成功提取marketing.sql，包含 {len(result['marketing.sql'])} 条语句")
                except Exception as e:
                    print(f"[WARN] 解析marketing.sql失败: {str(e)}")
                    result['marketing.sql'] = user_config['marketing.sql']
            
            # 提取pipeline.name和设置TTL
            if 'pipeline.name' in user_config:
                pipeline_name = user_config['pipeline.name']
                result['pipeline.name'] = pipeline_name
                print(f"[INFO] 成功提取pipeline.name: {pipeline_name}")
                
                # 1. 首先尝试根据pipeline.name从映射关系获取TTL
                ttl = self._get_ttl_by_pipeline_name(pipeline_name)
                if ttl:
                    result['table.exec.state.ttl'] = ttl
                    print(f"[INFO] 根据pipeline.name设置table.exec.state.ttl: {ttl}")
                else:
                    # 2. 如果没有匹配的TTL，尝试从marketing.sql同级目录获取
                    if 'table.exec.state.ttl' in user_config:
                        result['table.exec.state.ttl'] = user_config['table.exec.state.ttl']
                        print(f"[INFO] 从配置中获取table.exec.state.ttl: {result['table.exec.state.ttl']}")
                    else:
                        print(f"[INFO] 未找到table.exec.state.ttl配置，将不设置TTL值")
            
            # 如果没有pipeline.name，仍然尝试从配置中获取TTL
            elif 'table.exec.state.ttl' in user_config:
                result['table.exec.state.ttl'] = user_config['table.exec.state.ttl']
                print(f"[INFO] 从配置中获取table.exec.state.ttl: {result['table.exec.state.ttl']}")
        
        if 'marketing.ddl' not in result and 'marketing.sql' not in result:
            print(f"[WARN] 未找到marketing.ddl或marketing.sql配置")
        
        return result
    
    def get_savepoint_path(self, checkpoints_info):
        """
        
        
        从checkpoint信息中获取savepoint路径
        
        @param checkpoints_info: checkpoint信息
        @return: savepoint路径，如果没有则返回None
        """
        print(f"[INFO] 提取savepoint路径")
        
        # 检查是否有savepoint信息
        if 'latest' in checkpoints_info and 'savepoint' in checkpoints_info['latest']:
            savepoint = checkpoints_info['latest']['savepoint']
            if 'external_path' in savepoint:
                path = savepoint['external_path']
                print(f"[INFO] 获取到savepoint路径: {path}")
                return path
        
        print(f"[WARN] 未找到savepoint路径")
        return None
    
    def transform_config(self, marketing_config, savepoint_path=None):
        """
        
        
        转换配置为指定格式
        
        @param marketing_config: marketing配置
        @param savepoint_path: savepoint路径
        @return: 转换后的配置
        """
        print(f"[INFO] 转换配置为指定格式")
        
        # Base64编码
        program_args = base64.b64encode(
            json.dumps(marketing_config, ensure_ascii=False).encode('utf-8')
        ).decode('utf-8')
        
        # 构建转换后的结构
        result = {
            "entryClass": "com.quick.marketing.MarketingSQLTask",
            "parallelism": None,
            "programArgs": program_args
        }
        
        # 添加savepoint路径(如果有)
        if savepoint_path:
            result["savepointPath"] = savepoint_path
        
        return result
    
    def process_job(self, job_id):
        """
        
        
        处理单个作业
        
        @param job_id: 作业ID(JID)
        @return: 处理结果，包含作业ID和转换后的配置
        """
        print(f"[INFO] 处理作业: {job_id}")
        
        try:
            # 1. 获取作业配置
            job_config = self.get_job_config(job_id)
            
            # 2. 提取marketing配置
            marketing_config = self.extract_marketing_config(job_config)
            
            if not marketing_config:
                # 尝试从作业名称判断是否为ETL作业
                job_name = None
                if 'name' in job_config:
                    job_name = job_config['name']
                
                # 如果仍然没有找到名称，使用"unknown"作为名称
                if not job_name:
                    job_name = "unknown"
                
                # 检查是否为ETL作业
                if ETLMapping.is_etl_job(job_name):
                    print(f"[INFO] 作业 {job_id} ({job_name}) 没有marketing配置但被识别为ETL作业")
                    
                    # 获取checkpoint信息
                    try:
                        checkpoints = self.get_job_checkpoints(job_id)
                        savepoint_path = self.get_savepoint_path(checkpoints)
                    except Exception as e:
                        print(f"[WARN] 获取checkpoint信息失败: {str(e)}")
                        savepoint_path = None
                    
                    # ETL作业处理逻辑
                    etl_info = ETLMapping.get_etl_info(job_name)
                    etl_config = {
                        # 如果没有匹配到ETL信息，使用空字符串
                        "etl": etl_info if etl_info else ""
                    }
                    
                    # 添加savepoint路径(如果有)
                    if savepoint_path:
                        etl_config["savepoint"] = savepoint_path
                    
                    if not etl_info:
                        print(f"[WARN] 作业 {job_name} 被识别为ETL作业，但未能匹配到具体ETL类型，使用空etl字段")
                        
                    return {
                        "job_id": job_id,
                        "job_name": job_name,
                        "config": etl_config,
                        "is_etl": True
                    }
                else:
                    print(f"[ERROR] 未能从作业配置中提取到marketing配置")
                    return None
            
            # 3. 获取checkpoint信息
            try:
                checkpoints = self.get_job_checkpoints(job_id)
                savepoint_path = self.get_savepoint_path(checkpoints)
            except Exception as e:
                print(f"[WARN] 获取checkpoint信息失败: {str(e)}")
                savepoint_path = None
            
            # 获取作业名称，优先使用marketing_config中的pipeline.name
            job_name = marketing_config.get("pipeline.name", "")
            
            # 如果没有pipeline.name，尝试从作业配置中获取name字段
            if not job_name and 'name' in job_config:
                job_name = job_config['name']
            
            # 如果仍然没有找到名称，使用"unknown"作为名称
            if not job_name:
                job_name = "unknown"
                
            # 判断是否为ETL作业（没有DDL和SQL，或名称在ETL映射表中）
            is_etl = (('marketing.ddl' not in marketing_config or not marketing_config['marketing.ddl']) and 
                    ('marketing.sql' not in marketing_config or not marketing_config['marketing.sql'])) or \
                    ETLMapping.is_etl_job(job_name)
            
            if is_etl:
                # ETL作业处理逻辑
                etl_info = ETLMapping.get_etl_info(job_name)
                etl_config = {
                    # 如果没有匹配到ETL信息，使用空字符串
                    "etl": etl_info if etl_info else ""
                }
                
                # 添加savepoint路径(如果有)
                if savepoint_path:
                    etl_config["savepoint"] = savepoint_path
                
                if not etl_info:
                    print(f"[WARN] 作业 {job_name} 被识别为ETL作业，但未能匹配到具体ETL类型，使用空etl字段")
                    
                return {
                    "job_id": job_id,
                    "job_name": job_name,
                    "config": etl_config,
                    "is_etl": True
                }
            else:
                # SQL作业处理逻辑
                transformed_config = self.transform_config(marketing_config, savepoint_path)
                
                return {
                    "job_id": job_id,
                    "job_name": job_name,
                    "config": transformed_config,
                    "is_etl": False
                }
            
        except Exception as e:
            print(f"[ERROR] 处理作业 {job_id} 时发生错误: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return None
    
    def process_all(self, job_ids=None, output_file=None, etl_output_file=None, replay_mode=False, replay_timestamp=None, save_raw_data=True):
        """
        
        
        处理所有作业，支持实时查询和历史数据重放
        
        @param job_ids: 指定要处理的作业ID列表，如果为None则处理所有FINISHED状态的作业
        @param output_file: 输出文件路径，如果为None则不保存
        @param etl_output_file: ETL作业输出文件路径
        @param replay_mode: 是否使用重放模式
        @param replay_timestamp: 指定重放的数据时间戳
        @param save_raw_data: 是否保存原始数据(仅在非重放模式下有效)
        @return: 处理结果
        """
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  开始处理作业...")
        
        # 如果没有指定作业ID，获取所有FINISHED状态的作业
        if job_ids is None:
            if replay_mode:
                # 重放模式：从历史数据加载
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  使用重放模式，加载历史数据...")
                jobs_data = self.load_raw_jobs_data(replay_timestamp)
                
                if not jobs_data:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][ERROR]:  重放模式加载数据失败")
                    return {}
                    
                # 筛选FINISHED状态的作业
                finished_jobs = [job for job in jobs_data if job.get('state') == 'FINISHED']
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  从重放数据中找到 {len(finished_jobs)} 个FINISHED状态的作业")
            else:
                # 正常模式：实时查询API
                finished_jobs = self.get_finished_jobs()
                
            job_ids = [job['jid'] for job in finished_jobs]
        
        if not job_ids:
            print(f"[ERROR] 没有作业需要处理")
            return {}
        
        # 处理每个作业
        sql_results = {}  # 常规SQL作业结果
        etl_results = {}  # ETL作业结果
        
        for job_id in job_ids:
            result = self.process_job(job_id)
            if result:
                # 使用"job_id-job_name"作为键
                key = f"{job_id}-{result['job_name']}"
                
                if result.get('is_etl', False):
                    # ETL作业
                    etl_results[key] = result['config']
                else:
                    # SQL作业
                    sql_results[key] = result['config']
        
        # 输出SQL作业处理结果
        if sql_results:
            print(f"[INFO] 成功处理 {len(sql_results)} 个SQL作业")
            
            # 保存到文件
            if output_file:
                os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(sql_results, f, indent=2, ensure_ascii=False)
                print(f"[INFO] SQL作业结果已保存到: {output_file}")
                
                # 打印结果预览
                if len(sql_results) > 0:
                    print(f"\n[INFO] SQL作业结果预览(第一个作业):")
                    job_id = list(sql_results.keys())[0]
                    config = sql_results[job_id]
                    
                    print("-" * 50)
                    print(f"作业ID: {job_id}")
                    print(f"entryClass: {config['entryClass']}")
                    print(f"parallelism: {config['parallelism']}")
                    print(f"programArgs: {config['programArgs'][:30]}... (Base64编码，已截断)")
                    
                    if 'savepointPath' in config:
                        print(f"savepointPath: {config['savepointPath']}")
                    else:
                        print(f"savepointPath: (未找到)")
                    print("-" * 50)
                    
        # 输出ETL作业处理结果
        if etl_results:
            print(f"[INFO] 成功处理 {len(etl_results)} 个ETL作业")
            
            # 保存到文件
            if etl_output_file:
                os.makedirs(os.path.dirname(os.path.abspath(etl_output_file)), exist_ok=True)
                with open(etl_output_file, 'w', encoding='utf-8') as f:
                    json.dump(etl_results, f, indent=2, ensure_ascii=False)
                print(f"[INFO] ETL作业结果已保存到: {etl_output_file}")
                
                # 打印结果预览
                if len(etl_results) > 0:
                    print(f"\n[INFO] ETL作业结果预览(第一个作业):")
                    job_id = list(etl_results.keys())[0]
                    config = etl_results[job_id]
                    
                    print("-" * 50)
                    print(f"作业ID: {job_id}")
                    print(f"ETL: {config.get('etl', 'Unknown')}")
                    
                    if 'savepoint' in config:
                        print(f"savepoint: {config['savepoint']}")
                    else:
                        print(f"savepoint: (未找到)")
                    print("-" * 50)
                    
        if not sql_results and not etl_results:
            print(f"[ERROR] 没有成功处理的作业")
            
        return {
            "sql_jobs": sql_results,
            "etl_jobs": etl_results
        }

def parse_arguments():
    """
    
    
    解析命令行参数
    
    @return: 解析后的参数对象
    """
    parser = argparse.ArgumentParser(description='Flink作业处理工具')
    
    # 基本参数
    parser.add_argument('--job-id', type=str, help='指定作业ID(JID)，如不指定则处理所有FINISHED状态的作业')
    parser.add_argument('--output', type=str, default='output/processed_jobs.json', help='SQL作业输出文件路径')
    parser.add_argument('--etl-output', type=str, default='output/etl_jobs.json', help='ETL作业输出文件路径')
    parser.add_argument('--base-url', type=str, default='http://127.0.0.1:8081', help='Flink API基地址')
    parser.add_argument('--username', type=str, default='admin', help='API认证用户名')
    parser.add_argument('--password', type=str, default='admin', help='API认证密码')
    
    # 数据保存和重放相关参数
    parser.add_argument('--no-save-raw', action='store_true', help='禁用原始数据保存功能（默认会保存）')
    parser.add_argument('--replay', action='store_true', help='启用重放模式，使用保存的原始数据')
    parser.add_argument('--replay-timestamp', type=str, help='指定要重放的数据时间戳，格式为YYYYMMDD_HHMMSS')
    parser.add_argument('--list-replays', action='store_true', help='列出所有可用的重放数据集')
    
    return parser.parse_args()

def main():
    """
    
    
    主函数
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"===== Flink作业处理工具 (执行时间: {timestamp}) =====")
    
    # 解析命令行参数
    args = parse_arguments()
    
    # 创建处理器
    processor = FlinkJobProcessor(
        base_url=args.base_url,
        username=args.username,
        password=args.password
    )
    
    # 检查是否只是列出可用的重放数据集
    if args.list_replays:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  列出所有可用的重放数据集...")
        replays = processor.list_available_replays()
        if replays:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  共找到 {len(replays)} 个可用的重放数据集")
            print("\n使用方法: 通过 --replay --replay-timestamp TIMESTAMP 参数重放指定数据集")
        return 0 if replays else 1
    
    # 处理作业
    if args.job_id:
        # 处理单个指定作业
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  处理指定的作业: {args.job_id}")
        job_ids = [args.job_id]
    else:
        # 处理所有FINISHED作业
        if args.replay:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  使用重放模式处理历史数据")
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  处理所有FINISHED状态的作业")
        job_ids = None
    
    # 执行处理
    results = processor.process_all(
        job_ids=job_ids,
        output_file=args.output,
        etl_output_file=args.etl_output,
        replay_mode=args.replay,
        replay_timestamp=args.replay_timestamp,
        save_raw_data=not args.no_save_raw  # 如果指定了--no-save-raw则不保存原始数据
    )
    
    # 输出结果统计
    sql_count = len(results.get('sql_jobs', {}))
    etl_count = len(results.get('etl_jobs', {}))
    
    mode_desc = "重放模式" if args.replay else "实时查询模式"
    if args.replay and args.replay_timestamp:
        mode_desc += f"(时间戳: {args.replay_timestamp})"
    
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  处理完成({mode_desc})，共 {sql_count + etl_count} 个作业 (SQL: {sql_count}, ETL: {etl_count})")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  SQL作业输出文件: {args.output}")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkJobProcessor][INFO]:  ETL作业输出文件: {args.etl_output}")
    print(f"===== 处理结束 =====")
    
    return 0 if results else 1

if __name__ == "__main__":
    sys.exit(main())

