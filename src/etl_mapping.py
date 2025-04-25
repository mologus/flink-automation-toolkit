#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
ETL作业映射工具
定义作业名称与ETL jar包的对应关系
"""


class ETLMapping:
    """
    
    
    ETL作业映射类
    提供作业名称与ETL jar包的映射关系
    """
    
    # ETL作业名称与jar包的映射关系
    ETL_MAPPINGS = {
        "etl_marketing_job": {
            "description": "账户数据源",
            "jar": "etl-marketing-flink-ETLMarketingTask-1.0-SNAPSHOT.jar"
        },
        "pattern_etl_marketing_job": {
            "description": "TP数据源",
            "jar": "pattern_etl_marketing_job.jar"
        },
        "game_job_1": {
            "description": "游戏1数据源",
            "jar": "game-job1-6.1-SNAPSHOT.jar"
        },
        "game_job_2": {
            "description": "游戏2数据源",
            "jar": "game-job2-db.jar"
        },
        "mic_etl_kafka_job": {
            "description": "麦位数据源",
            "jar": "etl-mic-1.0-SNAPSHOT.jar"
        },
        "talent_etl_job": {
            "description": "主播数据源",
            "jar": "talent_etl_job.jar"
        },
        "user_behavior_job": {
            "description": "用户数据源",
            "jar": "user_behavior-4.0-SNAPSHOT.jar"
        },
        "analysis_user_job": {
            "description": "用户分析",
            "jar": "analysis_user_job.jar"
        },
        "analysis_order_job": {
            "description": "订单充值",
            "jar": "analysis_order_job.jar"
        },
        "mission_job": {
            "description": "任务数据源（基础任务系统）",
            "jar": "mission_job.jar"
        },
        "user_mission_job": {
            "description": "任务数据源（平台任务完成同步）",
            "jar": "user_mission_job.jar"
        },
        "activity_risk_control": {
            "description": "活动风控计算",
            "jar": "ActivityInspect-1.0-SNAPSHOT.jar"
        },
        "history_rate_job": {
            "description": "历史评分",
            "jar": "history-rate-1.0-SNAPSHOT.jar"
        },
        "active_user_job": {
            "description": "活跃用户",
            "jar": "active_user_info.jar"
        },
        "reel_operation_job": {
            "description": "视频操作",
            "jar": "reel-1.0-SNAPSHOT.jar"
        },
        "salary_job": {
            "description": "工资单",
            "jar": "salary-flink-SalaryJob-1.0.0.jar"
        },
        "app1_operation_etl_job": {
            "description": "App1-账户数据源",
            "jar": "app1-etl-operation-flink-ETLTask-1.0-SNAPSHOT.jar"
        },
        "app1_analysis_job": {
            "description": "App1-用户和订单数据源",
            "jar": "app1-analysis-1.0-SNAPSHOT.jar"
        },
        "app1_user_behavior_job": {
            "description": "App1-用户相关数据源",
            "jar": "app1-user_behavior-1.0-SNAPSHOT.jar"
        },
        "game_total_job": {
            "description": "所有游戏数据源",
            "jar": "game_total_job-SNAPSHOT.jar"
        },
        "mission_etl_job": {
            "description": "所有任务数据源",
            "jar": "mission_etl_job-1.0-SNAPSHOT.jar"
        },
        "common_etl_job": {
            "description": "其他数据源",
            "jar": "common_etl_job-1.0-SNAPSHOT.jar"
        }
    }
    
    @classmethod
    def get_etl_info(cls, job_name):
        """
        
        
        根据作业名称获取ETL信息
        
        @param job_name: 作业名称
        @return: ETL信息，包含描述和jar包，如果没有匹配则返回None
        """
        if not job_name:
            return None
            
        # 尝试精确匹配
        if job_name in cls.ETL_MAPPINGS:
            info = cls.ETL_MAPPINGS[job_name]
            return f"{info['description']} {job_name} |{info['jar']}"
        
        # 处理带有数字后缀的特殊情况
        # 例如 ta_game_job_1 应匹配到 ta_game_job
        parts = job_name.split('_')
        for etl_name, info in cls.ETL_MAPPINGS.items():
            # 检查是否为带数字后缀的ETL作业
            etl_parts = etl_name.split('_')
            if len(parts) > len(etl_parts):
                # 比较基础部分是否一致
                if '_'.join(parts[:len(etl_parts)]) == etl_name:
                    return f"{info['description']} {etl_name} |{info['jar']}"
        
        # 处理特殊情况 game_job 可能匹配到 game_job_1 或 game_job_2
        if job_name == "game_job":
            # 默认返回 game_job_1 的信息
            info = cls.ETL_MAPPINGS["game_job_1"]
            return f"{info['description']} game_job_1 |{info['jar']}"
            
        # 尝试部分匹配（作业名称可能包含更多信息）
        for etl_name, info in cls.ETL_MAPPINGS.items():
            if etl_name in job_name:
                return f"{info['description']} {etl_name} |{info['jar']}"
        
        # 未找到匹配
        return None
    
    @classmethod
    def is_etl_job(cls, job_name):
        """
        
        
        判断作业是否为ETL作业
        
        @param job_name: 作业名称
        @return: 是否为ETL作业
        """
        if not job_name:
            return False
            
        # 精确匹配
        if job_name in cls.ETL_MAPPINGS:
            return True
        
        # 处理带有数字后缀的特殊情况
        # 例如 ta_game_job_1 应匹配到 ta_game_job
        parts = job_name.split('_')
        for etl_name in cls.ETL_MAPPINGS.keys():
            etl_parts = etl_name.split('_')
            
            # 如果作业名称的部分与ETL名称完全匹配
            if len(parts) > len(etl_parts):
                # 检查前面的部分是否匹配, 例如 ta_game_job_1 的前3部分是否等于 ta_game_job
                if '_'.join(parts[:len(etl_parts)]) == etl_name:
                    print(f"[INFO] 作业 {job_name} 被识别为ETL作业 {etl_name} 的变体")
                    return True
        
        # 部分匹配
        for etl_name in cls.ETL_MAPPINGS.keys():
            if etl_name in job_name:
                return True
        
        return False

