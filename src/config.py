#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

生成时间: 2025/4/9 17:56:10

配置模块，提供默认配置和配置加载功能
"""

import os
import json
from .logger import setup_logger

logger = setup_logger("ConfigModule")

# 默认配置
DEFAULT_CONFIG = {
    "api": {
        "base_url": "http://127.0.0.1:8081",
        "username": "admin",
        "password": "admin",
        "timeout": 10
    },
    "output": {
        "directory": "./output",
        "default_filename": "flink_config.json"
    },
    "batch": {
        "size": 5,
        "retry_count": 3,
        "retry_delay": 2  # 重试延迟（秒）
    }
}


def load_config(config_file=None):
    """
    
    
    加载配置，可选从文件覆盖默认值
    
    @param config_file: 配置文件路径（可选），如果提供则从文件加载配置
    @return: 完整的配置字典
    """
    config = DEFAULT_CONFIG.copy()
    
    if config_file and os.path.exists(config_file):
        logger.info(" Loading configuration from %s", config_file)
        try:
            with open(config_file, 'r') as f:
                file_config = json.load(f)
                # 递归更新配置
                update_config(config, file_config)
        except Exception as e:
            logger.error(" Failed to load configuration file: %s", str(e))
    
    return config


def update_config(base_config, new_config):
    """
    
    
    递归更新配置字典
    
    @param base_config: 基础配置字典
    @param new_config: 新配置字典，用于覆盖基础配置
    """
    for key, value in new_config.items():
        if key in base_config and isinstance(base_config[key], dict) and isinstance(value, dict):
            # 如果两边都是字典，则递归更新
            update_config(base_config[key], value)
        else:
            # 否则直接覆盖
            base_config[key] = value
