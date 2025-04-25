# Flink ETL作业批量启动工具使用指南

本工具可以批量启动Flink ETL作业，通过读取处理后的ETL作业配置文件，使用对应的特定JAR包来启动每个ETL作业。支持启动全部作业或指定作业名称，并可设置启动间隔时间，避免对集群造成过大压力。

## 功能特点

- 从配置文件（默认`output/etl_jobs.json`）中加载ETL作业配置
- 根据ETL作业名称自动识别并使用对应的特定JAR包
- 支持启动所有ETL作业或指定特定作业
- 可设置每个作业启动后的等待间隔时间（默认5秒）
- 支持干运行模式(dry-run)，预览将要执行的操作而不实际执行
- 完整的错误处理和日志记录
- 自动重试机制，确保API请求的可靠性

## 使用方法

### 基本用法

启动配置文件中的所有ETL作业：

```bash
python flink_etl_starter.py --all
```

启动指定的一个或多个ETL作业：

```bash
python flink_etl_starter.py --job-name xxxetl_marketing_job,ta_game_job_1
```

使用干运行模式，只显示将要执行的操作，不实际启动作业：

```bash
python flink_etl_starter.py --all --base-url https://flink.xxx.com --username admin --password admin --dry-run
```

### 完整参数说明

```
参数:
  --config-file CONFIG_FILE  作业配置文件路径 (默认: output/etl_jobs.json)
  --job-name JOB_NAME        指定要启动的作业名称，多个名称用逗号分隔
  --all                      启动配置文件中的所有ETL作业
  --dry-run                  不实际启动作业，只打印要执行的操作
  --base-url BASE_URL        Flink API基地址 (默认: http://127.0.0.1:8081)
  --username USERNAME        API认证用户名 (默认: admin)
  --password PASSWORD        API认证密码 (默认: admin)
  --interval INTERVAL        启动作业之间的间隔时间(秒) (默认: 5)
```

### 示例用法

1. 启动所有ETL作业，每次启动后等待10秒：

```bash
python flink_etl_starter.py --all --interval 10
```

2. 从自定义配置文件启动作业：

```bash
python flink_etl_starter.py --all --config-file custom_etl_jobs.json
```

3. 使用自定义认证信息启动作业：

```bash
python flink_etl_starter.py --all --username admin --password secret123
```

4. 先预览要启动的作业，然后再实际执行：

```bash
# 先预览
python flink_etl_starter.py --all --dry-run

# 确认无误后执行
python flink_etl_starter.py --all --base-url https://flink.xxx.com --username admin --password admin
```

## 工作流程

1. 连接到Flink API
2. 从配置文件加载ETL作业配置
3. 如果指定了`--job-name`参数，筛选出指定的作业配置
4. 依次对每个作业执行启动操作：
   - 根据作业名称从ETL映射中获取对应的JAR包名称
   - 从Flink集群获取JAR包的ID
   - 向JAR的run API端点发送POST请求，包含保存点路径等参数
   - 等待作业启动响应
   - 如果有下一个作业，等待指定的间隔时间
5. 汇总并输出启动结果

## ETL作业与JAR包对应关系

本工具根据作业名称自动识别对应的JAR包，当前支持以下ETL作业类型：

1. 账户数据源 (xxxetl_marketing_job)
2. TP数据源 (xxxpattern_etl_marketing_job)
3. game1数据源 (ta_game_job_1)
4. game2数据源 (ta_game_job_2)
5. 麦位数据源 (xxxmic_etl_kafka_job)
6. 主播数据源 (ta_talent_etl_job)
7. 用户数据源 (ta_user_behavior_job)
8. 用户数据源 (ta_analysis_job)
9. 订单充值 (ta_analysis_order_job)
10. 任务数据源（老的任务系统同步任务）(ta_mission_job)
11. 任务数据源（营销平台任务完成同步）(sem_user_mission_job)
12. 活动风控计算 (activity_risk_control)
13. 历史评分 (ta_history_rate_job)
14. 活跃用户 (ta_active_user_job)
15. 视频操作 (oyelite_ta_reel_operation_job)
16. 工资单 (talent_live_duration_gift_mq_flink)

如果需要添加新的ETL作业类型，请在`src/etl_mapping.py`文件中更新`ETL_MAPPINGS`字典。

## 配置文件格式

工具所需的配置文件格式如下（通常由`flink_job_processor.py`生成）：

```json
{
  "8d673f013e8c42be5d75af21bcdf6532-xxxetl_marketing_job": {
    "etl": "账户数据源 xxxetl_marketing_job |xxx-etl-marketing-flink-xxxETLMarketingTask-1.0-SNAPSHOT-prod-0410084144.jar",
    "savepoint": "s3://xxx/flink/savepoint-8d673f-92a02953e875"
  },
  "f4d821ec4bf80ab5221fe8a933d7e976-ta_game_job_1": {
    "etl": "game1数据源 ta_game_job_1 |ta-game-job1-6.1-SNAPSHOT-prod-0228024033.jar",
    "savepoint": "s3://xxx/flink/savepoint-f4d821-78c03d44a159"
  }
}
```

其中：
- 对象键格式为 "作业ID-作业名称"
- "etl" 字段包含ETL作业描述和JAR包名称
- "savepoint" 字段为保存点路径

## 最佳实践

- 在生产环境中执行前，先使用`--dry-run`参数预览操作
- 为避免集群负载突增，根据作业复杂度合理设置`--interval`参数
- 批量启动前，确保集群有足够的资源处理这些作业
- 确保所有ETL作业的JAR包已上传到Flink集群
- 对于重要的作业，建议保存每次部署的配置和日志，以便回滚
- 确保作业配置文件包含正确的配置，可以先处理单个作业进行测试

## 特殊处理说明

- 对于旧版本名称（如`ta_game_job`）会自动映射到新版本的作业（如`ta_game_job_1`）
- JAR包名称支持部分匹配，如果没有找到完全匹配的JAR，会尝试查找包含指定名称的JAR

## 注意事项

- 启动大量作业可能会对Flink JobManager造成负载
- API认证信息需要具有启动作业的权限
- 确保使用正确的JAR名称，错误的JAR名称会导致启动失败
- 由于启动操作需要加载恢复点，可能会消耗较多资源
- 部署前确认配置文件中的savepointPath是否有效
- 确保savepointPath是可访问的，被启动作业需要权限访问此路径

## 故障排除

- 如果认证失败，检查用户名和密码是否正确
- 如果找不到JAR，确认JAR名称是否正确并检查是否已上传到集群
- 如果作业启动失败，检查savepoint路径是否有效
- 如果作业没有对应的ETL映射，检查`src/etl_mapping.py`中的`ETL_MAPPINGS`是否已包含该作业
- 如果集群资源不足，尝试增加启动间隔或减少并行启动的作业数
- 如果配置文件不正确，重新使用`flink_job_processor.py`生成
- 检查日志中的具体错误信息，通常会包含问题的详细说明
