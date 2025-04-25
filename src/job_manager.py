#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

生成时间: 2025/4/9 17:57:45

Flink任务管理模块
提供任务筛选和批量处理功能
"""

import concurrent.futures
import re
from .logger import setup_logger


class JobManager:
    """
    
    
    Flink任务管理类，负责任务筛选和批量处理
    """
    
    def __init__(self, api_client, batch_size=5):
        """
        
        
        初始化任务管理器
        
        @param api_client: FlinkApiClient实例
        @param batch_size: 批处理任务的并发数量
        """
        self.api_client = api_client
        self.batch_size = batch_size
        self.logger = setup_logger("JobManager")
        
    def get_all_jobs(self):
        """
        
        
        获取所有任务列表
        
        @return: 任务列表
        """
        self.logger.info(" Fetching all jobs")
        try:
            jobs = self.api_client.get_jobs_overview()
            self.logger.info(" Successfully retrieved %s jobs", len(jobs))
            return jobs
        except Exception as e:
            self.logger.error(" Failed to get all jobs: %s", str(e))
            raise
    
    def filter_jobs(self, jobs=None, job_name=None, job_id=None, custom_filter=None):
        """
        
        
        按条件筛选任务
        
        @param jobs: 任务列表，如果为None，则自动获取所有任务
        @param job_name: 按名称筛选，支持精确匹配和模糊匹配（使用*通配符）
        @param job_id: 按任务ID筛选
        @param custom_filter: 自定义筛选条件，格式为"key=value"或字典
        @return: 筛选后的任务列表
        """
        if jobs is None:
            jobs = self.get_all_jobs()
            
        self.logger.info(" Filtering jobs from a list of %s jobs", len(jobs))
        
        # 按ID筛选
        if job_id:
            self.logger.info(" Filtering by job ID: %s", job_id)
            jobs = [job for job in jobs if job.get('jid') == job_id]
            
        # 按名称筛选
        if job_name:
            self.logger.info(" Filtering by job name: %s", job_name)
            # 支持通配符匹配
            if '*' in job_name:
                pattern = job_name.replace('*', '.*')
                regex = re.compile(pattern)
                jobs = [job for job in jobs if regex.search(job.get('name', ''))]
            else:
                # 精确匹配
                jobs = [job for job in jobs if job.get('name') == job_name]
        
        # 自定义条件筛选
        if custom_filter:
            self.logger.info(" Applying custom filter")
            if isinstance(custom_filter, str):
                # 解析"key=value"格式
                key, value = custom_filter.split('=', 1)
                jobs = [job for job in jobs if str(job.get(key)) == value]
            elif isinstance(custom_filter, dict):
                # 使用字典格式的多个条件
                for key, value in custom_filter.items():
                    jobs = [job for job in jobs if str(job.get(key)) == str(value)]
        
        self.logger.info(" Filter result: %s jobs matched", len(jobs))
        return jobs
    
    def process_job(self, job_id):
        """
        
        
        处理单个任务，获取其配置信息
        
        @param job_id: 任务ID
        @return: 包含任务ID和配置的元组 (job_id, config)
        """
        self.logger.info(" Processing job: %s", job_id)
        try:
            config = self.api_client.get_job_config(job_id)
            return job_id, config
        except Exception as e:
            self.logger.error(" Failed to process job %s: %s", job_id, str(e))
            return job_id, None
    
    def process_jobs_batch(self, job_ids):
        """
        
        
        批量处理多个任务，使用线程池并发获取配置
        
        @param job_ids: 任务ID列表
        @return: 任务ID和配置的字典 {job_id: config}
        """
        if not job_ids:
            self.logger.warning(" No jobs to process")
            return {}
        
        result = {}
        total_jobs = len(job_ids)
        self.logger.info(" Processing %s jobs with batch size %s", 
                        total_jobs, self.batch_size)
        print(f"[DEBUG] 开始处理 {total_jobs} 个作业，批处理大小为 {self.batch_size}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.batch_size) as executor:
            # 提交所有任务到线程池
            future_to_id = {executor.submit(self.process_job, job_id): job_id for job_id in job_ids}
            
            # 处理完成的任务
            completed = 0
            success_count = 0
            failure_count = 0
            
            for future in concurrent.futures.as_completed(future_to_id):
                job_id = future_to_id[future]
                try:
                    job_id, config = future.result()
                    if config:
                        result[job_id] = config
                        self.logger.info(" Successfully processed job %s", job_id)
                        print(f"[DEBUG] 成功处理作业 {job_id}")
                        success_count += 1
                    else:
                        self.logger.warning(" Failed to get config for job %s", job_id)
                        print(f"[DEBUG] 无法获取作业配置 {job_id}")
                        failure_count += 1
                except Exception as e:
                    self.logger.error(" Exception occurred while processing job %s: %s", 
                                      job_id, str(e))
                    print(f"[DEBUG ERROR] 处理作业 {job_id} 时出错: {str(e)}")
                    import traceback
                    print(traceback.format_exc())
                    failure_count += 1
                
                completed += 1
                progress_percent = (completed / total_jobs) * 100
                self.logger.info(" Progress: %s/%s jobs processed (%.1f%%)", 
                               completed, total_jobs, progress_percent)
                print(f"[DEBUG] 进度: {completed}/{total_jobs} 作业已处理 ({progress_percent:.1f}%)")
        
        self.logger.info(" Batch processing completed. Successful: %s/%s, Failed: %s/%s", 
                        success_count, total_jobs, failure_count, total_jobs)
        print(f"[DEBUG] 批处理完成! 成功: {success_count}/{total_jobs}, 失败: {failure_count}/{total_jobs}")
        
        return result

