#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

生成时间: 2025/4/9 18:05:45

Flink配置提取工具
用于从Flink REST API获取作业配置并提取关键信息
"""

import os
import sys
import argparse
import json

from src.config import load_config
from src.api_client import FlinkApiClient
from src.mock_api_client import MockFlinkApiClient
from src.http_client_api import HttpClientFlinkApiClient
from src.job_manager import JobManager
from src.data_processor import ConfigProcessor
from src.logger import setup_logger


def parse_arguments():
    """
    
    
    解析命令行参数
    
    @return: 解析后的参数对象
    """
    parser = argparse.ArgumentParser(description='Flink任务配置提取工具')
    
    # API选项
    parser.add_argument('--base-url', type=str, help='API基地址，默认https://xxx')
    parser.add_argument('--username', type=str, help='API认证用户名')
    parser.add_argument('--password', type=str, help='API认证密码')
    
    # 模拟模式选项
    parser.add_argument('--mock', action='store_true', help='使用模拟模式，从本地文件读取数据而不是调用实际API')
    parser.add_argument('--mock-data-dir', type=str, default='mock_data', help='模拟数据目录')
    
    # 任务筛选选项
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--all', action='store_true', help='处理所有可用的jobs')
    group.add_argument('--job-name', type=str, help='按名称筛选job（支持*通配符）')
    group.add_argument('--job-id', type=str, help='直接指定job ID')
    
    parser.add_argument('--filter', type=str, help='使用自定义条件筛选jobs，格式为KEY=VALUE')
    
    # 输出选项
    parser.add_argument('--output-dir', type=str, help='指定输出目录')
    parser.add_argument('--combine', action='store_true', help='将所有结果合并为一个文件')
    parser.add_argument('--output-file', type=str, help='指定输出文件名（与--combine一起使用）')
    
    # 处理选项
    parser.add_argument('--batch-size', type=int, help='批处理时的并发请求数量')
    parser.add_argument('--config-file', type=str, help='配置文件路径')
    parser.add_argument('--use-http-client', action='store_true', help='使用HTTP Client API（解决认证问题）')
    
    return parser.parse_args()

def main():
    """
    
    
    主函数，处理命令行参数并执行配置提取流程
    """
    # 设置主logger
    logger = setup_logger("FlinkConfigExtractor")
    logger.info(" Starting Flink Config Extractor")
    
    # 添加控制台直接输出
    print("[DEBUG] Starting Flink Config Extractor")
    
    # 解析命令行参数
    args = parse_arguments()
    
    try:
        # 加载配置
        config = load_config(args.config_file)
        
        # 使用命令行参数覆盖配置
        if args.base_url:
            config['api']['base_url'] = args.base_url
        if args.username:
            config['api']['username'] = args.username
        if args.password:
            config['api']['password'] = args.password
        if args.output_dir:
            config['output']['directory'] = args.output_dir
        if args.batch_size:
            config['batch']['size'] = args.batch_size
            
        logger.info(" Configuration loaded")
            
        # 初始化API客户端 (根据是否模拟模式选择不同的客户端)
        if args.mock:
            # 使用模拟客户端
            # 使用直接路径，不进行复杂的路径计算
            mock_data_dir = args.mock_data_dir
            # 确保路径存在
            print(f"[DEBUG] Mock data dir: {mock_data_dir}")
            print(f"[DEBUG] Mock data dir exists: {os.path.exists(mock_data_dir)}")
            print(f"[DEBUG] Mock jobs overview exists: {os.path.exists(os.path.join(mock_data_dir, 'jobs_overview.json'))}")
            print(f"[DEBUG] Mock job config exists: {os.path.exists(os.path.join(mock_data_dir, f'job_{args.job_id}_config.json'))}")
            
            # 绝对路径
            abs_mock_data_dir = os.path.abspath(mock_data_dir)
            print(f"[DEBUG] Absolute mock data dir: {abs_mock_data_dir}")
            
            logger.info(" Using mock API client with data directory: %s", mock_data_dir)
            api_client = MockFlinkApiClient(mock_data_dir=mock_data_dir)
        else:
            # 根据参数选择使用标准API客户端或HTTP客户端API
            if args.use_http_client:
                logger.info(" Using HTTP Client API with base URL: %s", config['api']['base_url'])
                print(f"[DEBUG] 使用HTTP Client API客户端，基础URL：{config['api']['base_url']}")
                api_client = HttpClientFlinkApiClient(
                    base_url=config['api']['base_url'],
                    username=config['api']['username'],
                    password=config['api']['password'],
                    timeout=config['api']['timeout'],
                    max_retries=config['batch']['retry_count'],
                    retry_delay=config['batch']['retry_delay']
                )
            else:
                # 使用标准API客户端
                logger.info(" Using standard API client with base URL: %s", config['api']['base_url'])
                print(f"[DEBUG] 使用标准API客户端，基础URL：{config['api']['base_url']}")
                api_client = FlinkApiClient(
                    base_url=config['api']['base_url'],
                    username=config['api']['username'],
                    password=config['api']['password'],
                    timeout=config['api']['timeout'],
                    max_retries=config['batch']['retry_count'],
                    retry_delay=config['batch']['retry_delay']
                )
        
        # 初始化任务管理器
        job_manager = JobManager(
            api_client=api_client,
            batch_size=config['batch']['size']
        )
        
        # 初始化配置处理器
        config_processor = ConfigProcessor(
            output_dir=config['output']['directory']
        )
        
        # 获取任务列表
        jobs = job_manager.get_all_jobs()
        
        # 筛选任务
        if args.job_id:
            logger.info(" Filtering by job ID: %s", args.job_id)
            filtered_jobs = job_manager.filter_jobs(jobs, job_id=args.job_id)
        elif args.job_name:
            logger.info(" Filtering by job name: %s", args.job_name)
            filtered_jobs = job_manager.filter_jobs(jobs, job_name=args.job_name)
        elif args.filter:
            logger.info(" Filtering by custom filter: %s", args.filter)
            filtered_jobs = job_manager.filter_jobs(jobs, custom_filter=args.filter)
        elif args.all:
            logger.info(" Processing all %s jobs", len(jobs))
            filtered_jobs = jobs
        else:
            logger.error(" No filtering option specified. Please use --job-id, --job-name, --filter, or --all")
            sys.exit(1)
            
        if not filtered_jobs:
            logger.warning(" No jobs matched the filter criteria")
            sys.exit(0)
            
        logger.info(" Found %s matching jobs", len(filtered_jobs))
        
        # 提取job IDs
        job_ids = [job['jid'] for job in filtered_jobs]
        
        # 批量获取任务配置
        logger.info(" Fetching configurations for %s jobs", len(job_ids))
        print(f"[DEBUG] Fetching configurations for {len(job_ids)} jobs: {job_ids}")
        job_configs = job_manager.process_jobs_batch(job_ids)
        print(f"[DEBUG] Got job configs: {list(job_configs.keys())}")
        
        # 处理配置数据
        results = config_processor.process_job_configs(job_configs)
        print(f"[DEBUG] Processed results: {list(results.keys())}")
        
        # 检查输出目录
        output_dir = config['output']['directory']
        print(f"[DEBUG] Output directory: {output_dir}")
        print(f"[DEBUG] Output directory exists: {os.path.exists(output_dir)}")
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 保存结果
        output_filename = args.output_file or config['output']['default_filename']
        print(f"[DEBUG] Output filename: {output_filename}")
        saved_files = config_processor.save_multiple_results(
            results, 
            combine=args.combine, 
            output_filename=output_filename
        )
        print(f"[DEBUG] Saved files: {saved_files}")
        
        if saved_files:
            logger.info(" Results saved to:")
            for file_path in saved_files:
                logger.info("   - %s", file_path)
        else:
            logger.warning(" No results were saved")
            
        logger.info(" Processing completed successfully")
        
    except Exception as e:
        logger.error(" Error during execution: %s", str(e), exc_info=True)
        print(f"[DEBUG ERROR] {str(e)}")
        import traceback
        print(traceback.format_exc())
        sys.exit(1)
        
    return 0

if __name__ == "__main__":
    sys.exit(main())

