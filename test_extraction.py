#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
直接测试配置提取和保存功能
"""

import os
import json
import sys
from src.mock_api_client import MockFlinkApiClient
from src.job_manager import JobManager
from src.data_processor import ConfigProcessor

def main():
    print("===== 测试配置提取和保存流程 =====")
    
    # 1. 设置测试参数
    mock_data_dir = "mock_data"
    job_id = "ce38fe58413a61c4b2250594ff816bb9"
    output_dir = "output"
    output_file = "test_result.json"
    
    print(f"作业ID: {job_id}")
    print(f"模拟数据目录: {mock_data_dir}")
    print(f"输出目录: {output_dir}")
    
    try:
        # 2. 初始化模拟API客户端
        print("\n初始化模拟API客户端...")
        api_client = MockFlinkApiClient(mock_data_dir=mock_data_dir)
        
        # 3. 初始化作业管理器
        print("\n初始化作业管理器...")
        job_manager = JobManager(api_client=api_client, batch_size=1)
        
        # 4. 获取作业配置
        print(f"\n获取作业配置 {job_id}...")
        job_config = api_client.get_job_config(job_id)
        print(f"获取到配置: {job_config is not None}")
        
        # 将配置放入字典
        job_configs = {job_id: job_config}
        print(f"作业配置数量: {len(job_configs)}")
        
        # 5. 初始化配置处理器
        print(f"\n初始化配置处理器...")
        config_processor = ConfigProcessor(output_dir=output_dir)
        
        # 6. 处理配置数据
        print(f"\n处理配置数据...")
        results = config_processor.process_job_configs(job_configs)
        print(f"处理结果数量: {len(results)}")
        print(f"结果键: {list(results.keys())}")
        
        # 7. 保存结果
        print(f"\n保存结果到文件...")
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 直接保存单个文件
        print(f"保存到单个文件: {output_file}")
        saved_files = config_processor.save_multiple_results(
            results, 
            combine=True,
            output_filename=output_file
        )
        
        print(f"保存的文件: {saved_files}")
        
        # 8. 验证文件是否存在
        expected_file = os.path.join(output_dir, output_file)
        print(f"\n验证输出文件: {expected_file}")
        print(f"文件存在: {os.path.exists(expected_file)}")
        
        if os.path.exists(expected_file):
            with open(expected_file, 'r') as f:
                content = json.load(f)
            print(f"文件内容键: {list(content.keys())}")
            print(f"文件大小: {os.path.getsize(expected_file)} 字节")
        
        print("\n===== 测试完成 =====")
        
    except Exception as e:
        print(f"测试过程中出错: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return 1
        
    return 0

if __name__ == "__main__":
    sys.exit(main())
