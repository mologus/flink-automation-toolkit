#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Flink作业转换工具演示脚本
演示了获取作业信息并转换为新格式的完整流程
"""

import os
import json
import base64
from flink_job_transformer import FlinkJobTransformer


def demo_transform():
    """
    
    
    演示转换流程
    """
    print("===== Flink作业转换工具演示 =====")
    
    # 步骤1: 准备演示目录
    print("\n步骤1: 准备演示目录")
    os.makedirs("demo_output", exist_ok=True)
    
    # 步骤2: 创建示例输入数据(如果不存在)
    print("\n步骤2: 检查示例输入数据")
    input_file = "examples/sample_input.json"
    output_file = "demo_output/transformed_jobs.json"
    
    if not os.path.exists(input_file):
        print(f"  错误: 示例输入文件 {input_file} 不存在")
        print("  请先运行创建示例输入文件的步骤")
        return
    else:
        print(f"  找到示例输入文件: {input_file}")
        
        # 显示输入文件内容概述
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            print(f"  输入文件包含 {len(data)} 个作业配置")
            for job_id, config in data.items():
                ddl_count = len(config.get('marketing.ddl', []))
                sql_count = len(config.get('marketing.sql', []))
                print(f"  - 作业 {job_id}: {ddl_count} 个DDL, {sql_count} 个SQL语句")
    
    # 步骤3: 创建模拟Flink API响应(已实际准备好)
    print("\n步骤3: 转换作业配置")
    print("  使用http://127.0.0.1:8081作为Flink API地址")
    print("  实际环境中，这会从真实的Flink API获取数据")
    print("  以下是转换过程的演示：")
    
    # 步骤4: 转换单个作业配置示例
    print("\n步骤4: 单个作业转换示例")
    
    # 读取作业配置
    with open(input_file, 'r', encoding='utf-8') as f:
        job_configs = json.load(f)
    
    # 获取第一个作业
    job_id = list(job_configs.keys())[0]
    job_config = job_configs[job_id]
    
    # 模拟savepoint路径
    savepoint_path = "s3://xxx/flink/savepoint-b18d18-92a02953e875"
    
    # 创建配置副本，并进行转换
    config_for_encoding = {
        "marketing.ddl": job_config.get("marketing.ddl", []),
        "marketing.sql": job_config.get("marketing.sql", []),
        "pipeline.name": job_config.get("pipeline.name", "")
    }
    
    # Base64编码
    encoded_config = base64.b64encode(
        json.dumps(config_for_encoding, ensure_ascii=False).encode('utf-8')
    ).decode('utf-8')
    
    # 构建转换后的结构
    transformed_config = {
        "entryClass": "com.quick.marketing.MarketingSQLTask",
        "parallelism": None,
        "programArgs": encoded_config,
        "savepointPath": savepoint_path
    }
    
    # 显示转换后的配置(部分)
    print(f"  作业ID: {job_id}")
    print(f"  转换后的配置:")
    print(f"  - entryClass: {transformed_config['entryClass']}")
    print(f"  - parallelism: {transformed_config['parallelism']}")
    print(f"  - programArgs: {encoded_config[:30]}... (Base64编码，已截断)")
    print(f"  - savepointPath: {transformed_config['savepointPath']}")
    
    # 步骤5: 使用转换工具处理所有作业
    print("\n步骤5: 使用转换工具处理所有作业")
    print("  这将实际连接到Flink API并处理所有作业")
    print("  命令: python flink_job_transformer.py --input examples/sample_input.json --output demo_output/transformed_jobs.json")
    
    # 创建转换器实例
    transformer = FlinkJobTransformer(
        base_url="http://127.0.0.1:8081",
        username="flink",
        password="flink-!@xxx2021"
    )
    
    # 转换所有作业(实际处理)
    print("\n开始实际处理...")
    success_count = transformer.process_all(
        input_file=input_file,
        output_file=output_file
    )
    
    # 步骤6: 查看结果
    print("\n步骤6: 查看转换结果")
    
    if os.path.exists(output_file) and success_count > 0:
        print(f"  成功创建输出文件: {output_file}")
        
        with open(output_file, 'r', encoding='utf-8') as f:
            result_data = json.load(f)
            
        print(f"  输出文件包含 {len(result_data)} 个已转换的作业配置")
        
        for job_id, config in result_data.items():
            print(f"  - 作业 {job_id}:")
            print(f"    * entryClass: {config.get('entryClass', 'N/A')}")
            print(f"    * savepointPath: {config.get('savepointPath', 'N/A')}")
            
            # 解码programArgs以验证内容
            try:
                program_args = config.get('programArgs', '')
                decoded = json.loads(base64.b64decode(program_args).decode('utf-8'))
                
                ddl_count = len(decoded.get('marketing.ddl', []))
                sql_count = len(decoded.get('marketing.sql', []))
                
                print(f"    * programArgs: 包含 {ddl_count} 个DDL和 {sql_count} 个SQL语句(已验证Base64解码)")
            except Exception as e:
                print(f"    * programArgs: 无法解码 ({str(e)})")
    else:
        print(f"  没有找到输出文件或处理失败: {output_file}")
    
    print("\n===== 演示完成 =====")
    print("要运行实际转换，请使用以下命令:")
    print("python flink_job_transformer.py --input <输入文件> --output <输出文件> --base-url <API地址>")

if __name__ == "__main__":
    demo_transform()

