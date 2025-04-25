#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
简单测试脚本，用于验证模拟数据读取和文件写入功能
"""

import os
import json
import sys

def main():
    print("=== 测试模拟数据读取和文件写入 ===")
    
    # 1. 检查模拟数据目录
    mock_data_dir = "mock_data"
    print(f"模拟数据目录: {mock_data_dir}")
    print(f"目录存在: {os.path.exists(mock_data_dir)}")
    
    # 2. 尝试读取jobs_overview.json
    jobs_overview_path = os.path.join(mock_data_dir, "jobs_overview.json")
    print(f"\n尝试读取作业概览: {jobs_overview_path}")
    print(f"文件存在: {os.path.exists(jobs_overview_path)}")
    
    if os.path.exists(jobs_overview_path):
        try:
            with open(jobs_overview_path, 'r', encoding='utf-8') as f:
                jobs_data = json.load(f)
                print(f"成功读取作业概览, 包含 {len(jobs_data.get('jobs', []))} 个作业")
                job_ids = [job['jid'] for job in jobs_data.get('jobs', [])]
                print(f"作业ID: {job_ids}")
        except Exception as e:
            print(f"读取作业概览时出错: {str(e)}")
    
    # 3. 尝试读取特定作业配置
    job_id = "ce38fe58413a61c4b2250594ff816bb9"
    job_config_path = os.path.join(mock_data_dir, f"job_{job_id}_config.json")
    print(f"\n尝试读取作业配置: {job_config_path}")
    print(f"文件存在: {os.path.exists(job_config_path)}")
    
    if os.path.exists(job_config_path):
        try:
            with open(job_config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
                print(f"成功读取作业配置")
                print(f"作业名称: {config_data.get('name')}")
                
                # 检查是否包含marketing.ddl
                user_config = config_data.get('execution-config', {}).get('user-config', {})
                has_ddl = 'marketing.ddl' in user_config
                has_sql = 'marketing.sql' in user_config
                print(f"包含marketing.ddl: {has_ddl}")
                print(f"包含marketing.sql: {has_sql}")
        except Exception as e:
            print(f"读取作业配置时出错: {str(e)}")
    
    # 4. 测试输出目录
    output_dir = "output"
    print(f"\n输出目录: {output_dir}")
    print(f"目录存在: {os.path.exists(output_dir)}")
    
    # 确保输出目录存在
    try:
        os.makedirs(output_dir, exist_ok=True)
        print(f"已确保输出目录存在")
    except Exception as e:
        print(f"创建输出目录时出错: {str(e)}")
    
    # 5. 尝试写入测试文件
    test_output_path = os.path.join(output_dir, "test_output.json")
    test_data = {"test": "data", "timestamp": "2025-04-10"}
    print(f"\n尝试写入测试文件: {test_output_path}")
    
    try:
        with open(test_output_path, 'w', encoding='utf-8') as f:
            json.dump(test_data, f, indent=4)
        print(f"成功写入测试文件")
        
        # 验证文件是否已写入
        print(f"测试文件存在: {os.path.exists(test_output_path)}")
    except Exception as e:
        print(f"写入测试文件时出错: {str(e)}")
    
    # 6. 显示当前工作目录和绝对路径
    print(f"\n当前工作目录: {os.getcwd()}")
    print(f"模拟数据目录绝对路径: {os.path.abspath(mock_data_dir)}")
    print(f"输出目录绝对路径: {os.path.abspath(output_dir)}")
    
    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    main()
