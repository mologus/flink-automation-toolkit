#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
from logging.handlers import RotatingFileHandler

def setup_logger(name, log_file=None, level=logging.INFO):
    """
    
    生成时间: 2025/4/9 17:55:00
    
    设置和配置logger实例
    
    @param name: logger的名称
    @param log_file: 日志文件路径(可选)，如果不提供则只输出到控制台
    @param level: 日志级别，默认为INFO
    @return: 配置好的logger实例
    """
    # 创建logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 定义日志格式
    formatter = logging.Formatter(
        '[%(asctime)s][%(name)s][%(levelname)s]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 添加控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 如果提供了日志文件路径，添加文件处理器
    if log_file:
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10*1024*1024, backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger
