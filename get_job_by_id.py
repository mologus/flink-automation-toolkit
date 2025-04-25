#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Flink作业参数获取工具
直接通过作业ID(JID)获取配置信息和savepoint路径
"""

import os
import sys
import json
import base64
import argparse
import http.client
import time
from src.etl_mapping import ETLMapping


class FlinkJobFetcher:
    """
    
    
    Flink作业参数获取类
    通过JID直接获取作业配置信息
    """
    
    def __init__(self, base_url, username="flink", password="flink-!@xxx2021"):
        """
        
        
        初始化获取工具
        
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
        
        print(f"[INFO] Flink作业参数获取工具初始化完成，API地址: {self.base_url}")
    
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
    
    def get_finished_jobs(self):
        """
        
        
        获取所有FINISHED状态的作业
        
        @return: 作业列表
        """
        print(f"[INFO] 获取所有FINISHED状态作业...")
        
        result = self._make_request("/jobs/overview")
        
        if 'jobs' in result:
            # 筛选状态为FINISHED的作业
            finished_jobs = [job for job in result['jobs'] if job.get('state') == 'FINISHED']
            print(f"[INFO] 找到 {len(finished_jobs)} 个FINISHED状态的作业(共 {len(result['jobs'])} 个作业)")
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
                except Exception as e:
                    print(f"[WARN] 解析marketing.sql失败: {str(e)}")
                    result['marketing.sql'] = user_config['marketing.sql']
            
            # 提取pipeline.name
            if 'pipeline.name' in user_config:
                result['pipeline.name'] = user_config['pipeline.name']
        
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
    
    def get_job_by_id(self, job_id, output_file=None):
        """
        
        
        通过作业ID获取并转换作业信息
        
        @param job_id: 作业ID(JID)
        @param output_file: 输出文件路径，不指定则只打印不保存
        @return: 转换后的配置
        """
        print(f"[INFO] 开始处理作业: {job_id}")
        
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
                    result = {
                        # 如果没有匹配到ETL信息，使用空字符串
                        "etl": etl_info if etl_info else ""
                    }
                    
                    # 添加savepoint路径(如果有)
                    if savepoint_path:
                        result["savepoint"] = savepoint_path
                    
                    if not etl_info:
                        print(f"[WARN] 作业 {job_name} 被识别为ETL作业，但未能匹配到具体ETL类型，使用空etl字段")
                    
                    # 构建最终输出
                    result_key = f"{job_id}-{job_name}"
                    final_result = {result_key: result}
                    print(f"[INFO] 作业被识别为ETL作业: {job_name}")
                    
                    return final_result
                else:
                    print(f"[ERROR] 未能从作业配置中提取到marketing配置")
                    return None
            
            print(f"[INFO] 成功提取marketing配置")
            print(f"  - DDL: {len(marketing_config.get('marketing.ddl', []))} 条")
            print(f"  - SQL: {len(marketing_config.get('marketing.sql', []))} 条")
            
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
            
            # 使用"job_id-job_name"作为键
            result_key = f"{job_id}-{job_name}"
            
            # 判断是否为ETL作业（没有DDL和SQL，或名称在ETL映射表中）
            is_etl = (('marketing.ddl' not in marketing_config or not marketing_config['marketing.ddl']) and 
                    ('marketing.sql' not in marketing_config or not marketing_config['marketing.sql'])) or \
                    ETLMapping.is_etl_job(job_name)
            
            if is_etl:
                # ETL作业处理逻辑
                etl_info = ETLMapping.get_etl_info(job_name)
                result = {
                    # 如果没有匹配到ETL信息，使用空字符串
                    "etl": etl_info if etl_info else ""
                }
                
                # 添加savepoint路径(如果有)
                if savepoint_path:
                    result["savepoint"] = savepoint_path
                
                if not etl_info:
                    print(f"[WARN] 作业 {job_name} 被识别为ETL作业，但未能匹配到具体ETL类型，使用空etl字段")
                    
                print(f"[INFO] 作业被识别为ETL作业: {job_name}")
            else:
                # SQL作业处理逻辑
                result = self.transform_config(marketing_config, savepoint_path)
                print(f"[INFO] 作业被识别为SQL作业: {job_name}")
            
            # 构建最终输出
            final_result = {result_key: result}
            
            # 输出结果
            formatted_json = json.dumps(final_result, indent=2, ensure_ascii=False)
            print("\n[INFO] 转换结果:")
            print("-" * 50)
            print(formatted_json)
            print("-" * 50)
            
            # 保存到文件(如果指定)
            if output_file:
                os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(formatted_json)
                print(f"[INFO] 结果已保存到: {output_file}")
            
            job_type = "ETL" if is_etl else "SQL"
            return {
                "result_key": result_key,
                "config": result,
                "job_type": job_type
            }
            
        except Exception as e:
            print(f"[ERROR] 处理作业时发生错误: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return None

def parse_arguments():
    """
    
    
    解析命令行参数
    
    @return: 解析后的参数对象
    """
    parser = argparse.ArgumentParser(description='Flink作业参数获取工具')
    
    parser.add_argument('--job-id', type=str, required=True, help='作业ID(JID)')
    parser.add_argument('--output', type=str, help='输出文件路径')
    parser.add_argument('--base-url', type=str, default='http://127.0.0.1:8081', help='Flink API基地址')
    parser.add_argument('--username', type=str, default='flink', help='API认证用户名')
    parser.add_argument('--password', type=str, default='flink-!@xxx2021', help='API认证密码')
    parser.add_argument('--print-only', action='store_true', help='仅打印结果，不保存到文件')
    
    return parser.parse_args()

def main():
    """
    
    
    主函数
    """
    print("===== Flink作业参数获取工具 =====")
    
    # 解析命令行参数
    args = parse_arguments()
    
    # 创建获取工具
    fetcher = FlinkJobFetcher(
        base_url=args.base_url,
        username=args.username,
        password=args.password
    )
    
    # 通过作业ID获取参数
    output_file = None if args.print_only else args.output
    result = fetcher.get_job_by_id(
        job_id=args.job_id,
        output_file=output_file
    )
    
    if result:
        print(f"[INFO] 作业参数获取成功")
        return 0
    else:
        print(f"[ERROR] 作业参数获取失败")
        return 1

if __name__ == "__main__":
    sys.exit(main())

