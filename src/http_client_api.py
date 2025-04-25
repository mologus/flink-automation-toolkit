#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

生成时间: 2025/4/10 11:19:00

基于http.client的Flink REST API客户端
提供与Flink API的低级别HTTP连接
"""

import time
import json
import http.client
from .logger import setup_logger


class HttpClientFlinkApiClient:
    """
    
    
    使用http.client的Flink REST API客户端
    完全匹配成功的API测试请求格式
    """
    
    def __init__(self, base_url, username, password, timeout=10, max_retries=3, retry_delay=2):
        """
        
        
        初始化客户端
        
        @param base_url: API基础URL，例如 http://127.0.0.1:8081
        @param username: Basic Auth用户名
        @param password: Basic Auth密码
        @param timeout: 请求超时时间（秒）
        @param max_retries: 最大重试次数
        @param retry_delay: 重试间隔（秒）
        """
        # 解析主机名和协议
        self.base_url = base_url.rstrip('/')
        if self.base_url.startswith('https://'):
            self.host = self.base_url[8:]  # 移除 'https://'
            self.use_https = True
        else:
            self.host = self.base_url[7:]  # 移除 'http://'
            self.use_https = False
            
        self.username = username
        self.password = password
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = setup_logger("HttpClientFlinkApiClient")
        
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
        
        self.logger.info(" Initialized HTTP Client API with host: %s", self.host)
        protocol = "HTTPS" if self.use_https else "HTTP"
        print(f"[DEBUG] HTTP API客户端初始化: {self.host} ({protocol})")
        print(f"[DEBUG] 认证方式: Basic Auth")
    
    def _make_request(self, path, method='GET'):
        """
        
        
        使用http.client发送请求
        
        @param path: API路径
        @param method: HTTP方法 (GET, POST等)
        @return: 响应JSON数据
        """
        full_path = path
        retries = 0
        
        self.logger.info(" Making %s request to %s", method, full_path)
        print(f"[DEBUG] 发送请求: {method} {full_path}")
        
        while retries <= self.max_retries:
            try:
                # 创建连接
                if self.use_https:
                    conn = http.client.HTTPSConnection(self.host, timeout=self.timeout)
                else:
                    conn = http.client.HTTPConnection(self.host, timeout=self.timeout)
                
                # 发送请求
                conn.request(method, full_path, headers=self.headers)
                
                # 获取响应
                response = conn.getresponse()
                data = response.read().decode('utf-8')
                
                # 输出响应信息
                status = response.status
                print(f"[DEBUG] 响应状态码: {status}")
                
                if status == 200:
                    # 解析JSON响应
                    result = json.loads(data)
                    self.logger.info(" Request successful with status code %s", status)
                    return result
                else:
                    self.logger.error(" Request failed with status code %s", status)
                    print(f"[DEBUG ERROR] 请求失败，状态码: {status}")
                    print(f"[DEBUG] 响应内容: {data[:200]}...")
                    
                    if status == 401:
                        print(f"[DEBUG ERROR] 认证失败(401 Unauthorized)：请检查用户名和密码是否正确")
                        raise Exception("Authentication failed")
                    raise Exception(f"Request failed with status code {status}")
                    
            except Exception as e:
                self.logger.error(" Error making request: %s", str(e))
                print(f"[DEBUG ERROR] 请求错误: {str(e)}")
                
            finally:
                # 关闭连接
                if 'conn' in locals():
                    conn.close()
            
            # 重试逻辑
            retries += 1
            if retries <= self.max_retries:
                self.logger.warning(
                    " Retrying in %s seconds... (Attempt %s of %s)",
                    self.retry_delay, retries, self.max_retries
                )
                time.sleep(self.retry_delay)
            else:
                self.logger.error(
                    " Max retries (%s) reached. Request failed.",
                    self.max_retries
                )
                break
        
        # 如果所有重试都失败，抛出异常
        raise Exception(f"Failed to make request to {full_path} after {self.max_retries} retries")
    
    def get_jobs_overview(self):
        """
        
        
        获取所有作业概览
        
        @return: 作业概览列表
        """
        self.logger.info(" Getting jobs overview")
        
        # 尝试不同的API路径
        api_paths = ["/jobs/overview", "/overview", "/jobs", "/"]
        for path in api_paths:
            try:
                print(f"[DEBUG] 尝试获取作业概览: {path}")
                response = self._make_request(path)
                
                # 检查响应中是否包含jobs字段
                if 'jobs' in response:
                    self.logger.info(" Successfully retrieved %s jobs", len(response['jobs']))
                    return response['jobs']
                else:
                    self.logger.warning(" Response does not contain 'jobs' field")
                    print(f"[DEBUG] 响应不包含'jobs'字段. 响应内容: {json.dumps(response)[:200]}...")
            except Exception as e:
                self.logger.error(" Failed to get jobs overview from path %s: %s", path, str(e))
                print(f"[DEBUG] 从路径 {path} 获取作业概览失败: {str(e)}")
                # 尝试下一个路径
                continue
                
        # 如果所有路径都失败
        print(f"[DEBUG] 所有API路径尝试均失败。建议使用模拟模式: python main.py --mock --all")
        raise Exception("Failed to get jobs from all API paths. Please use --mock flag.")
    
    def get_job_config(self, job_id):
        """
        
        
        获取特定作业的配置信息
        
        @param job_id: 作业ID (JID)
        @return: 作业配置信息
        """
        if not job_id:
            error_msg = "Job ID cannot be empty"
            self.logger.error(" %s", error_msg)
            raise ValueError(error_msg)
            
        self.logger.info(" Getting config for job %s", job_id)
        
        try:
            response = self._make_request(f"/jobs/{job_id}/config")
            self.logger.info(" Successfully retrieved config for job %s", job_id)
            return response
        except Exception as e:
            self.logger.error(" Failed to get config for job %s: %s", job_id, str(e))
            raise

