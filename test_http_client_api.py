#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
使用http.client API客户端测试Flink API连接
"""

import os
import json
import sys
from src.http_client_api import HttpClientFlinkApiClient
from src.data_processor import ConfigProcessor

def main():
    """主函数"""
    print("=== 测试使用Http Client API连接Flink ===")
    
    # 设置API连接信息
    base_url = "xxx"
    username = "flink"
    password = "flink-!@xxx2021"
    output_dir = "output"
    output_file = "http_client_result.json"
    
    print(f"API地址: {base_url}")
    print(f"用户名: {username}")
    print(f"输出目录: {output_dir}")
    
    try:
        # 初始化API客户端
        print("\n初始化API客户端...")
        api_client = HttpClientFlinkApiClient(
            base_url=base_url,
            username=username,
            password=password,
            timeout=10,
            max_retries=2
        )
        
        # 获取所有作业
        print("\n获取作业列表...")
        try:
            jobs = api_client.get_jobs_overview()
            print(f"获取到 {len(jobs)} 个作业")
            
            if jobs:
                print("\n作业列表:")
                for i, job in enumerate(jobs):
                    job_id = job.get('jid', 'unknown')
                    job_name = job.get('name', 'unnamed')
                    print(f"  {i+1}. {job_name} (ID: {job_id})")
                
                # 处理前几个作业的配置
                job_configs = {}
                max_jobs = min(3, len(jobs))  # 最多处理3个作业
                
                for i in range(max_jobs):
                    job_id = jobs[i]['jid']
                    job_name = jobs[i]['name']
                    print(f"\n获取作业配置 {i+1}/{max_jobs}: {job_name} ({job_id})...")
                    
                    try:
                        job_config = api_client.get_job_config(job_id)
                        print(f"  配置获取成功，大小: {len(str(job_config))} 字符")
                        job_configs[job_id] = job_config
                    except Exception as e:
                        print(f"  获取配置失败: {str(e)}")
                
                if job_configs:
                    # 处理作业配置
                    print(f"\n处理 {len(job_configs)} 个作业配置...")
                    config_processor = ConfigProcessor(output_dir=output_dir)
                    results = config_processor.process_job_configs(job_configs)
                    
                    # 保存结果
                    print(f"\n保存结果到 {output_file}...")
                    os.makedirs(output_dir, exist_ok=True)
                    
                    saved_files = config_processor.save_multiple_results(
                        results,
                        combine=True,
                        output_filename=output_file
                    )
                    
                    print(f"保存的文件: {saved_files}")
                    
                    # 检验结果
                    output_path = os.path.join(output_dir, output_file)
                    if os.path.exists(output_path):
                        with open(output_path, 'r') as f:
                            content = json.load(f)
                        print(f"\n结果文件内容大小: {len(str(content))} 字符")
                        print(f"结果文件包含的作业ID: {list(content.keys())}")
                    else:
                        print(f"\n错误: 结果文件未创建")
                else:
                    print("\n没有成功获取到任何作业配置")
            else:
                print("\n没有找到作业")
                
        except Exception as e:
            print(f"获取作业列表失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
        
        print("\n=== 测试完成 ===")
        
    except Exception as e:
        print(f"测试过程中出错: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return 1
        
    return 0

if __name__ == "__main__":
    sys.exit(main())
