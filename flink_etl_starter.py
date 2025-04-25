#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Flink ETL作业批量启动工具
可一键启动所有ETL作业，使用对应的特定JAR包启动每个作业
"""

import os
import sys
import json
import time
import http.client
import argparse
import base64
from datetime import datetime
from src.etl_mapping import ETLMapping


class FlinkETLStarter:
    """
    
    
    Flink ETL作业启动类
    提供批量启动ETL作业的功能，每个作业使用特定的JAR包
    """
    
    def __init__(self, base_url, username="admin", password="admin", interval=5):
        """
        
        
        初始化启动工具
        
        @param base_url: API基础URL，例如 http://127.0.0.1:8081
        @param username: Basic Auth用户名
        @param password: Basic Auth密码
        @param interval: 启动作业间隔时间(秒)
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
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  Flink ETL作业启动工具初始化完成，API地址: {self.base_url}")
    
    def _make_request(self, path, method="GET", body=None):
        """
        
        
        发送HTTP请求到Flink API
        
        @param path: API路径
        @param method: HTTP方法 (GET, POST, etc.)
        @param body: 请求体，用于POST请求
        @return: 响应JSON对象
        """
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  请求API: {path} (方法: {method})")
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
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  请求成功，状态码: {status}")
                    return result
                else:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][ERROR]:  请求失败，状态码: {status}")
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][ERROR]:  响应内容: {data[:200]}...")
                    
                    if status == 401:
                        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][ERROR]:  认证失败，请检查用户名和密码")
                        raise Exception("认证失败")
                    raise Exception(f"请求失败，状态码: {status}")
                    
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][ERROR]:  请求错误: {str(e)}")
                
            finally:
                # 关闭连接
                if 'conn' in locals():
                    conn.close()
            
            # 重试逻辑
            retries += 1
            if retries <= max_retries:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  {retry_delay}秒后重试(第{retries}次)...")
                time.sleep(retry_delay)
            
        # 如果所有重试都失败，抛出异常
        raise Exception(f"请求失败，已重试{max_retries}次")
    
    def get_jar_info_by_name(self, jar_name):
        """
        
        
        根据JAR包名称获取对应的JAR信息，包括ID和入口类
        
        @param jar_name: JAR包名称
        @return: 包含JAR ID和入口类的字典，如果找不到则返回None
        """
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  查找JAR: {jar_name}")
        
        try:
            # 获取所有上传的JAR列表
            result = self._make_request("/jars")
            
            if 'files' in result:
                for jar_file in result['files']:
                    if 'id' in jar_file and 'name' in jar_file:
                        # 精确匹配
                        if jar_file['name'] == jar_name:
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  找到JAR ID: {jar_file['id']}")
                            entry_class = self._get_entry_class_from_jar_file(jar_file)
                            return {
                                'id': jar_file['id'],
                                'entry_class': entry_class,
                                'name': jar_file['name']
                            }
                
                # 如果没有精确匹配，尝试部分匹配
                for jar_file in result['files']:
                    if 'id' in jar_file and 'name' in jar_file:
                        # 部分匹配情况，例如jar_name只是文件名的一部分
                        if jar_name in jar_file['name']:
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  找到部分匹配的JAR ID: {jar_file['id']}")
                            entry_class = self._get_entry_class_from_jar_file(jar_file)
                            return {
                                'id': jar_file['id'],
                                'entry_class': entry_class,
                                'name': jar_file['name']
                            }
            
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][WARN]:  未找到JAR: {jar_name}")
            return None
            
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][ERROR]:  获取JAR列表失败: {str(e)}")
            return None
    
    def _get_entry_class_from_jar_file(self, jar_file):
        """
        
        
        从JAR文件信息中提取入口类名称
        
        @param jar_file: JAR文件信息字典
        @return: 入口类名称，如果没有则返回None
        """
        try:
            # 检查是否有entry数组并且不为空
            if 'entry' in jar_file and jar_file['entry'] and len(jar_file['entry']) > 0:
                # 取第一个入口类
                entry_class = jar_file['entry'][0]['name']
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  JAR {jar_file['name']} 的入口类: {entry_class}")
                return entry_class
            
            # 如果没有entry信息，尝试通过jar_file['id']请求获取
            jar_id = jar_file['id']
            jar_detail = self._make_request(f"/jars/{jar_id}")
            
            if jar_detail and 'entry' in jar_detail and jar_detail['entry'] and len(jar_detail['entry']) > 0:
                entry_class = jar_detail['entry'][0]['name']
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  JAR {jar_file['name']} 的入口类: {entry_class}")
                return entry_class
                
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][WARN]:  获取JAR入口类失败: {str(e)}")
        
        # 根据JAR名称推断入口类
        jar_name = jar_file['name'].lower()
        
        # 特殊情况处理
        entry_class_mapping = {
            'xxx-salary-flink-salaryjob': 'com.quick.salary.job.SalaryJob',
            'sem_user_mission_job': 'com.quick.ta.task.TaTask',
            'oyelite-reel': 'com.quick.ta.task.TaTask',
            'xxx-etl-marketing-flink': 'com.quick.etl.marketing.task.xxxETLMarketingTask',
            'activityinspect': 'com.xxx.ActivityInspect',
            'ta_mission_job': 'com.quick.ta.task.TaTask',
            'ta_history_rate_job': 'com.quick.ta.task.TaTask',
            'ta_active_user_info': 'com.quick.ta.task.TaTask',
            'xxxpattern_etl_marketing_job': 'com.quick.etl.marketing.task.xxxETLMarketingTask',
            'ta-game-job': 'com.quick.ta.task.TaTask',
            'user_behavior': 'com.quick.ta.task.TaTask',
            'marketing-flink-marketingsqltask': 'com.quick.marketing.MarketingSQLTask',
            'ta_talent_etl_job': 'com.quick.ta.task.TaTask',
            'xxx-etl-mic': 'com.quick.etl.marketing.task.xxxETLMarketingTask',
            'ta_analysis': 'com.quick.ta.task.TaTask',
            'xxx-ta-game': 'com.quick.ta.task.TaTask'
        }
        
        # 遍历映射尝试匹配
        for key, entry_class in entry_class_mapping.items():
            if key in jar_name:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  根据JAR名称 {jar_file['name']} 推断入口类: {entry_class}")
                return entry_class
        
        # 默认返回空
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][WARN]:  无法确定JAR {jar_file['name']} 的入口类")
        return None
    
    def start_etl_job(self, job_name, savepoint_path=None, entryClass=None):
        """
        
        
        启动ETL作业
        
        @param job_name: 作业名称
        @param savepoint_path: 保存点路径
        @param entryClass: 入口类（可选，如果提供则覆盖自动识别的入口类）
        @return: 启动请求结果
        """
        # 获取ETL作业信息
        etl_info = ETLMapping.get_etl_info(job_name)
        if not etl_info:
            raise Exception(f"未找到ETL作业信息: {job_name}")
        
        # 解析ETL信息
        parts = etl_info.split('|')
        if len(parts) != 2:
            raise Exception(f"ETL信息格式无效: {etl_info}")
        
        description, jar_name = parts
        jar_name = jar_name.strip()
        
        # 获取JAR信息（包括ID和入口类）
        jar_info = self.get_jar_info_by_name(jar_name)
        if not jar_info:
            raise Exception(f"未找到JAR信息: {jar_name}")
        
        jar_id = jar_info['id']
        auto_entry_class = jar_info['entry_class']
        actual_jar_name = jar_info['name']
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  启动ETL作业: {job_name}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  描述: {description}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  使用JAR: {actual_jar_name} (ID: {jar_id})")
        
        # 构建请求体
        body = {}
        
        # 优先使用传入的entryClass，如果没有则使用自动识别的入口类
        final_entry_class = entryClass if entryClass else auto_entry_class
        if final_entry_class:
            body["entryClass"] = final_entry_class
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  使用入口类: {final_entry_class}")
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][WARN]:  未找到入口类，使用JAR默认入口类")
        
        # 如果有保存点，设置保存点路径
        if savepoint_path:
            body["savepointPath"] = savepoint_path
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  使用保存点: {savepoint_path}")
            
        # 如果是编程参数类型的ETL作业，可以添加其他配置
        # 这里可以根据作业名称设置特定的参数
            
        # 发送启动请求
        return self._make_request(f"/jars/{jar_id}/run", method="POST", body=body)
    
    def load_job_configs(self, config_file):
        """
        
        
        加载作业配置
        
        @param config_file: 配置文件路径
        @return: 作业配置列表
        """
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  从文件加载ETL作业配置: {config_file}")
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                configs = json.load(f)
                
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  成功加载 {len(configs)} 个ETL作业配置")
            return configs
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][ERROR]:  加载配置文件失败: {str(e)}")
            return {}
    
    def start_all_jobs(self, configs, dry_run=False):
        """
        
        
        启动所有ETL作业
        
        @param configs: ETL作业配置字典
        @param dry_run: 是否只打印要执行的操作而不实际执行
        @return: 启动结果列表
        """
        if not configs:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][WARN]:  没有ETL作业需要启动")
            return []
        
        job_entries = list(configs.items())
        total_jobs = len(job_entries)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  准备启动 {total_jobs} 个ETL作业")
        
        if dry_run:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  干运行模式，不会实际启动作业")
            for i, (job_key, job_config) in enumerate(job_entries):
                job_name = job_key.split('-')[-1]  # 提取作业名称
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  [{i+1}/{total_jobs}] 将启动ETL作业: {job_name}")
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  保存点路径: {job_config.get('savepoint', 'None')}")
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  ETL信息: {job_config.get('etl', 'Unknown')}")
            return []
        
        results = []
        # 逐个启动作业
        for i, (job_key, job_config) in enumerate(job_entries):
            # 从作业键中提取作业名称
            job_name = job_key.split('-')[-1]  # 假设格式是 "id-name"
            savepoint = job_config.get('savepoint')
            
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  [{i+1}/{total_jobs}] 启动ETL作业: {job_name}")
            
            try:
                result = self.start_etl_job(job_name, savepoint_path=savepoint)
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  成功启动ETL作业: {job_name}")
                
                # 如果响应中有jobid，输出提示
                if isinstance(result, dict) and 'jobid' in result:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  新的作业ID: {result['jobid']}")
                
                results.append({
                    "job_name": job_name,
                    "success": True,
                    "result": result
                })
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][ERROR]:  启动ETL作业 {job_name} 失败: {str(e)}")
                results.append({
                    "job_name": job_name,
                    "success": False,
                    "error": str(e)
                })
            
            # 如果不是最后一个作业，等待一段时间
            if i < total_jobs - 1:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  等待 {self.interval} 秒后继续...")
                time.sleep(self.interval)
        
        # 统计结果
        success_count = sum(1 for r in results if r.get('success', False))
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  启动ETL作业完成，成功: {success_count}/{total_jobs}")
        
        return results


def parse_arguments():
    """
    
    
    解析命令行参数
    
    @return: 解析后的参数对象
    """
    parser = argparse.ArgumentParser(description='Flink ETL作业批量启动工具')
    
    parser.add_argument('--config-file', type=str, default='output/etl_jobs.json', help='作业配置文件')
    parser.add_argument('--job-name', type=str, help='指定要启动的作业名称，多个名称用逗号分隔')
    parser.add_argument('--all', action='store_true', help='启动配置文件中的所有ETL作业')
    parser.add_argument('--dry-run', action='store_true', help='不实际启动作业，只打印要执行的操作')
    parser.add_argument('--base-url', type=str, default='http://127.0.0.1:8081', help='Flink API基地址')
    parser.add_argument('--username', type=str, default='admin', help='API认证用户名')
    parser.add_argument('--password', type=str, default='admin', help='API认证密码')
    parser.add_argument('--interval', type=int, default=5, help='启动作业之间的间隔时间(秒)')
    
    return parser.parse_args()

def main():
    """
    
    
    主函数
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"===== Flink ETL作业批量启动工具 (执行时间: {timestamp}) =====")
    
    # 解析命令行参数
    args = parse_arguments()
    
    # 验证参数
    if not args.all and not args.job_name:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][ERROR]:  必须指定 --all 或 --job-name")
        return 1
    
    # 创建启动工具
    starter = FlinkETLStarter(
        base_url=args.base_url,
        username=args.username,
        password=args.password,
        interval=args.interval
    )
    
    # 加载配置
    all_configs = starter.load_job_configs(args.config_file)
    if not all_configs:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][ERROR]:  配置文件为空或无法加载")
        return 1
    
    # 处理作业启动
    if args.job_name:
        # 启动指定的作业
        job_names = [name.strip() for name in args.job_name.split(',') if name.strip()]
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  将启动 {len(job_names)} 个指定ETL作业")
        
        # 筛选指定的作业配置
        selected_configs = {}
        for job_key, job_config in all_configs.items():
            # 从作业键中提取作业名称
            job_name = job_key.split('-')[-1]  # 假设格式是 "id-name"
            if job_name in job_names:
                selected_configs[job_key] = job_config
        
        # 检查是否有未找到的作业
        found_names = [job_key.split('-')[-1] for job_key in selected_configs.keys()]
        missing = [name for name in job_names if name not in found_names]
        if missing:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][WARN]:  以下ETL作业在配置文件中未找到: {', '.join(missing)}")
        
        results = starter.start_all_jobs(selected_configs, dry_run=args.dry_run)
    else:
        # 启动所有作业
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  将启动配置文件中的所有ETL作业")
        results = starter.start_all_jobs(all_configs, dry_run=args.dry_run)
    
    if results:
        success_count = sum(1 for r in results if r.get('success', False))
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  启动ETL作业完成，成功: {success_count}/{len(results)}")
        return 0 if success_count == len(results) else 2
    else:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}][FlinkETLStarter][INFO]:  没有ETL作业被处理")
        return 0

if __name__ == "__main__":
    sys.exit(main())

