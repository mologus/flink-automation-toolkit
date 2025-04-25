#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

生成时间: 2025/4/9 18:04:30

Flink配置数据处理模块
负责从Flink任务配置中提取关键信息并处理输出
"""

import os
import json
from .logger import setup_logger


class ConfigProcessor:
    """
    
    
    配置处理器类，用于提取和处理Flink任务配置中的关键数据
    """
    
    def __init__(self, output_dir="./output"):
        """
        
        
        初始化配置处理器
        
        @param output_dir: 输出目录
        """
        self.output_dir = output_dir
        self.logger = setup_logger("ConfigProcessor")
        
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            self.logger.info(" Created output directory: %s", output_dir)
    
    def extract_marketing_config(self, job_config):
        """
        
        
        从作业配置中提取marketing相关信息
        
        @param job_config: 作业配置信息
        @return: 包含marketing.ddl、marketing.sql和pipeline.name的字典，如果未找到则对应值为None
        """
        self.logger.info(" Extracting marketing configuration")
        
        result = {
            "marketing.ddl": None,
            "marketing.sql": None,
            "pipeline.name": None
        }
        
        try:
            # 检查配置结构
            if not job_config or 'execution-config' not in job_config:
                self.logger.warning(" Invalid job config structure: missing 'execution-config'")
                return result
                
            exec_config = job_config['execution-config']
            if 'user-config' not in exec_config:
                self.logger.warning(" Invalid job config structure: missing 'user-config'")
                return result
                
            user_config = exec_config['user-config']
            
            # 提取marketing.ddl
            if 'marketing.ddl' in user_config:
                raw_ddl = user_config['marketing.ddl']
                # 如果是字符串形式，尝试解析JSON
                if isinstance(raw_ddl, str):
                    try:
                        result['marketing.ddl'] = json.loads(raw_ddl)
                        self.logger.info(" Successfully parsed marketing.ddl from string")
                    except json.JSONDecodeError:
                        # 如果不是有效的JSON，保留原始字符串
                        result['marketing.ddl'] = raw_ddl
                        self.logger.warning(" Could not parse marketing.ddl as JSON, using raw string")
                else:
                    # 如果已经是列表或字典，直接使用
                    result['marketing.ddl'] = raw_ddl
                    self.logger.info(" Using raw marketing.ddl object")
            else:
                self.logger.warning(" marketing.ddl not found in config")
                
            # 提取marketing.sql
            if 'marketing.sql' in user_config:
                raw_sql = user_config['marketing.sql']
                # 同样尝试解析JSON
                if isinstance(raw_sql, str):
                    try:
                        result['marketing.sql'] = json.loads(raw_sql)
                        self.logger.info(" Successfully parsed marketing.sql from string")
                    except json.JSONDecodeError:
                        result['marketing.sql'] = raw_sql
                        self.logger.warning(" Could not parse marketing.sql as JSON, using raw string")
                else:
                    result['marketing.sql'] = raw_sql
                    self.logger.info(" Using raw marketing.sql object")
            else:
                self.logger.warning(" marketing.sql not found in config")
                
            # 提取pipeline.name
            if 'pipeline.name' in user_config:
                result['pipeline.name'] = user_config['pipeline.name']
                self.logger.info(" Found pipeline.name: %s", result['pipeline.name'])
            else:
                self.logger.warning(" pipeline.name not found in config")
                
            return result
            
        except Exception as e:
            self.logger.error(" Error extracting marketing config: %s", str(e))
            return result
    
    def process_job_configs(self, jobs_configs):
        """
        
        
        处理多个作业配置并提取marketing信息
        
        @param jobs_configs: 作业ID和配置的字典 {job_id: config}
        @return: 处理后的结果字典 {job_id: extracted_data}
        """
        self.logger.info(" Processing %s job configurations", len(jobs_configs))
        
        results = {}
        skipped_jobs = 0
        
        for job_id, config in jobs_configs.items():
            try:
                # 添加作业ID和名称到结果中
                job_name = config.get('name', 'unknown')
                self.logger.info(" Processing job %s (%s)", job_id, job_name)
                
                # 提取marketing配置
                marketing_config = self.extract_marketing_config(config)
                
                # 如果job_name未在config中找到，尝试从config['jid']获取
                if job_name == 'unknown' and 'jid' in config:
                    job_name = config['jid']
                
                # 检查是否有marketing.ddl或marketing.sql，如果都没有则跳过该作业
                if marketing_config['marketing.ddl'] is None and marketing_config['marketing.sql'] is None:
                    self.logger.warning(" Skipping job %s (%s): No marketing.ddl or marketing.sql found",
                                      job_id, job_name)
                    print(f"[DEBUG] 跳过作业 {job_id} ({job_name}): 没有找到marketing.ddl或marketing.sql")
                    skipped_jobs += 1
                    continue
                
                # 添加到结果集
                results[job_id] = {
                    'marketing.ddl': marketing_config['marketing.ddl'],
                    'marketing.sql': marketing_config['marketing.sql'],
                    'pipeline.name': marketing_config['pipeline.name']
                }
                self.logger.info(" Successfully added job %s to results", job_id)
                
            except Exception as e:
                self.logger.error(" Error processing job %s: %s", job_id, str(e))
                skipped_jobs += 1
        
        self.logger.info(" Processed %s/%s jobs successfully, skipped %s jobs", 
                         len(results), len(jobs_configs), skipped_jobs)
        return results
    
    def save_to_json(self, data, filename):
        """
        
        
        将数据保存为JSON文件
        
        @param data: 要保存的数据
        @param filename: 文件名（不含路径）
        @return: 完整的文件路径
        """
        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)
        
        filepath = os.path.join(self.output_dir, filename)
        print(f"[DEBUG] Saving data to file: {filepath}")
        self.logger.info(" Saving data to %s", filepath)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            print(f"[DEBUG] File saved successfully: {filepath}")
            self.logger.info(" Successfully saved data to %s", filepath)
            return filepath
        except Exception as e:
            print(f"[DEBUG ERROR] Failed to save file: {str(e)}")
            self.logger.error(" Error saving data to %s: %s", filepath, str(e))
            import traceback
            print(traceback.format_exc())
            raise
    
    def save_multiple_results(self, results, combine=False, output_filename=None):
        """
        
        
        保存多个任务的处理结果
        
        @param results: 处理结果字典 {job_id: result_data}
        @param combine: 是否将所有结果合并到一个文件
        @param output_filename: 输出文件名（仅在combine=True时使用）
        @return: 保存的文件路径列表
        """
        if not results:
            self.logger.warning(" No results to save")
            return []
        
        saved_files = []
        
        if combine:
            # 合并所有结果到一个文件
            filename = output_filename or "combined_results.json"
            filepath = self.save_to_json(results, filename)
            saved_files.append(filepath)
            
        else:
            # 每个作业单独保存一个文件
            for job_id, result in results.items():
                job_name = result.get('name', job_id)
                # 使用作业名称或ID作为文件名
                safe_name = "".join([c if c.isalnum() else "_" for c in job_name])
                filename = f"{safe_name}_{job_id[:8]}.json"
                
                filepath = self.save_to_json(result, filename)
                saved_files.append(filepath)
        
        return saved_files

