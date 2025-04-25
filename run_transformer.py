#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Flink作业转换工具快速运行脚本
简单的方式运行转换工具
"""

import sys
from flink_job_transformer import FlinkJobTransformer, main

if __name__ == "__main__":
    # 方式1: 直接调用main函数(需要命令行参数)
    if len(sys.argv) > 1:
        sys.exit(main())
    
    # 方式2: 使用默认参数运行(无需命令行参数)
    print("===== 运行Flink作业转换工具 =====")
    print("使用默认参数运行...")
    
    # 使用示例输入文件
    input_file = "examples/sample_input.json"
    output_file = "output/transformed_jobs.json"
    
    # 创建转换器实例
    transformer = FlinkJobTransformer(
        base_url="http://127.0.0.1:8081",
        username="flink",
        password="flink-!@xxx2021"
    )
    
    # 处理所有作业
    success_count = transformer.process_all(
        input_file=input_file,
        output_file=output_file
    )
    
    print(f"===== 处理完成，成功处理 {success_count} 个作业 =====")
    
    # 方式3: 命令行方式(参考)
    print("\n要使用命令行方式运行，请使用:")
    print(f"python flink_job_transformer.py --input {input_file} --output {output_file} --base-url http://127.0.0.1:8081")
