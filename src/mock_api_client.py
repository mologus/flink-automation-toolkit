#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

生成时间: 2025/4/9 18:13:00

Flink REST API模拟客户端
用于在没有实际API连接的情况下进行测试
"""

import os
import json
from .logger import setup_logger


class MockFlinkApiClient:
    """
    
    
    Flink REST API模拟客户端类
    通过读取本地JSON文件模拟API响应
    """
    
    def __init__(self, mock_data_dir):
        """
        
        
        初始化模拟客户端
        
        @param mock_data_dir: 包含模拟数据文件的目录
        """
        self.mock_data_dir = mock_data_dir
        self.logger = setup_logger("MockFlinkApiClient")
        
        self.logger.info(" Initialized Mock Flink API client with mock data directory: %s", 
                         mock_data_dir)
        
    def get_jobs_overview(self):
        """
        
        
        获取所有作业概览（从模拟数据）
        
        @return: 作业概览列表
        """
        self.logger.info(" Getting mock jobs overview")
        
        try:
            # 读取模拟数据文件
            overview_file = os.path.join(self.mock_data_dir, "jobs_overview.json")
            with open(overview_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if 'jobs' in data:
                self.logger.info(" Successfully retrieved %s mock jobs", len(data['jobs']))
                return data['jobs']
            else:
                self.logger.warning(" Mock data does not contain 'jobs' field")
                return []
                
        except Exception as e:
            self.logger.error(" Failed to get mock jobs overview: %s", str(e))
            raise
    
    def get_job_config(self, job_id):
        """
        
        
        获取特定作业的配置信息（从模拟数据）
        
        @param job_id: 作业ID (JID)
        @return: 作业配置信息
        """
        if not job_id:
            error_msg = "Job ID cannot be empty"
            self.logger.error(" %s", error_msg)
            raise ValueError(error_msg)
            
        self.logger.info(" Getting mock config for job %s", job_id)
        print(f"[DEBUG] 获取作业配置: {job_id}")
        print(f"[DEBUG] 模拟数据目录: {self.mock_data_dir}")
        print(f"[DEBUG] 模拟数据目录存在: {os.path.exists(self.mock_data_dir)}")
        
        try:
            # 构建模拟数据文件路径
            config_file = os.path.join(self.mock_data_dir, f"job_{job_id}_config.json")
            abs_config_file = os.path.abspath(config_file)
            print(f"[DEBUG] 配置文件路径: {config_file}")
            print(f"[DEBUG] 配置文件绝对路径: {abs_config_file}")
            
            # 检查文件是否存在
            if not os.path.exists(config_file):
                self.logger.error(" Mock data file not found: %s", config_file)
                print(f"[DEBUG ERROR] 模拟数据文件不存在: {config_file}")
                
                # 列出模拟数据目录下的所有文件
                if os.path.exists(self.mock_data_dir):
                    print(f"[DEBUG] 模拟数据目录中的文件:")
                    for file in os.listdir(self.mock_data_dir):
                        print(f"  - {file}")
                        
                raise FileNotFoundError(f"Mock data file not found for job {job_id}")
                
            # 读取模拟数据
            print(f"[DEBUG] 正在读取配置文件...")
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            print(f"[DEBUG] 成功读取配置文件")
            if 'name' in config:
                print(f"[DEBUG] 作业名称: {config['name']}")
            
            if 'execution-config' in config and 'user-config' in config['execution-config']:
                user_config = config['execution-config']['user-config']
                print(f"[DEBUG] 用户配置包含 marketing.ddl: {'marketing.ddl' in user_config}")
                print(f"[DEBUG] 用户配置包含 marketing.sql: {'marketing.sql' in user_config}")
                
            self.logger.info(" Successfully retrieved mock config for job %s", job_id)
            return config
            
        except json.JSONDecodeError as e:
            self.logger.error(" Invalid JSON in mock config file: %s", str(e))
            print(f"[DEBUG ERROR] JSON解析错误: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(" Failed to get mock config for job %s: %s", job_id, str(e))
            print(f"[DEBUG ERROR] 获取配置失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
            raise

