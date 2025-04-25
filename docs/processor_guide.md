# Flink作业处理工具使用指南

本工具可以从Flink REST API获取作业信息，处理并转换为特定格式，完成从获取FINISHED状态作业列表、提取配置信息、获取savepoint路径到转换输出的端到端流程。

## 功能特点

- 自动获取所有FINISHED状态的作业列表
- 智能识别SQL作业和ETL作业，分别处理
- 从作业配置中提取marketing.ddl和marketing.sql信息
- 获取作业的savepoint/checkpoint路径
- 将配置转换为指定的新格式，包括Base64编码
- 完整的错误处理和日志记录
- 支持处理单个作业或批量处理多个作业

## 使用方法

### 基本用法

处理所有FINISHED状态的作业并保存结果：

```bash
python flink_job_processor.py --base-url https://flink.xxx.com --username admin --password admin
```

处理单个特定的作业：

```bash
python flink_job_processor.py --job-id 4f5d693c442711b7881dfc46ff557b8c
```

指定SQL作业和ETL作业的输出文件：

```bash
python flink_job_processor.py --base-url http://127.0.0.1:8081 --output processed_jobs.json --etl-output etl_jobs.json
```

### 完整参数说明

```
参数:
  --job-id JOB_ID       指定作业ID(JID)，如不指定则处理所有FINISHED状态的作业
  --output OUTPUT       SQL作业输出文件路径 (默认: output/processed_jobs.json)
  --etl-output ETL_OUTPUT  ETL作业输出文件路径 (默认: output/etl_jobs.json)
  --base-url BASE_URL   Flink API基地址 (默认: http://127.0.0.1:8081)
  --username USERNAME   API认证用户名 (默认: admin)
  --password PASSWORD   API认证密码 (默认: admin)
```

### 示例用法

1. 处理所有FINISHED状态的作业，使用自定义认证信息：

```bash
python flink_job_processor.py --base-url https://flink.example.com --username admin --password secret123
```

2. 处理特定作业，并将结果保存到指定位置：

```bash
python flink_job_processor.py --job-id ce38fe58413a61c4b2250594ff816bb9 --output results/my_job_result.json
```

## 工作流程

1. 连接到Flink API，获取所有作业列表
2. 筛选出状态为FINISHED的作业
3. 对每个作业：
   - 获取作业详细配置
   - 提取marketing.ddl、marketing.sql和pipeline.name
   - 获取作业的checkpoint信息
   - 提取savepoint路径
   - 转换为目标格式
4. 汇总结果并保存到输出文件

## 工作分类和输出格式

工具会自动将作业分为两类：SQL作业和ETL作业，分别输出到不同的文件中。

### SQL作业输出

SQL作业是指配置中包含marketing.ddl和marketing.sql的作业，输出格式如下：

```json
{
  "b18d18538ef206192f7764c48c34a5a1-mega_20250410_task": {
    "entryClass": "com.quick.marketing.MarketingSQLTask",
    "parallelism": null,
    "programArgs": "ewogICAgIm1hcmtldGluZy5kZG...(Base64编码的原始配置)",
    "savepointPath": "s3://xxx/flink/savepoint-b18d18-92a02953e875"
  },
  "4b2757789b9d3494d4757214d48d4df9-competition_20250414_ts_task": {
    "entryClass": "com.quick.marketing.MarketingSQLTask",
    "parallelism": null,
    "programArgs": "ewogICAgIm1hcmtldGluZy5kZG...(Base64编码的原始配置)",
    "savepointPath": "s3://another-savepoint-path"
  }
}
```

其中programArgs是对以下内容进行Base64编码得到的：

```json
{
  "marketing.ddl": [
    "CREATE TABLE kafka_source ...",
    "CREATE TABLE business_operation_events_sink ..."
  ],
  "marketing.sql": [
    "INSERT INTO business_operation_events_model_sink SELECT ..."
  ],
  "pipeline.name": "competition_20250414_ts_task"
}
```

### ETL作业输出

ETL作业是指配置中不包含marketing.ddl和marketing.sql的作业，或名称匹配已知ETL作业名称的作业。ETL作业输出格式如下：

```json
{
  "b18d18538ef206192f7764c48c34a5a1-xxxetl_marketing_job": {
    "etl": "账户数据源 xxxetl_marketing_job |xxx-etl-marketing-flink-1.0-operation-test-20230802-SNAPSHOT.jar",
    "savepoint": "s3://xxx/flink/savepoint-b18d18-92a02953e875"
  },
  "4b2757789b9d3494d4757214d48d4df9-ta_game_job": {
    "etl": "game数据源 ta_game_job |xxx-ta-Game-1.0-SNAPSHOT-test-1030062624.jar",
    "savepoint": "s3://another-savepoint-path"
  }
}
```

系统支持的ETL作业类型包括：

1. 账户数据源 (xxxetl_marketing_job)
2. TP数据源 (xxxpattern_etl_marketing_job)
3. game数据源 (ta_game_job)
4. 麦位数据源 (xxxmic_etl_kafka_job)
5. 主播数据源 (ta_talent_etl_job)
6. 用户数据源 (ta_user_behavior_job)
7. 用户数据源/order充值 (ta_analysis_job)
8. 任务数据源 (ta_mission_job, sem_user_mission_job)
9. 工资单 (talent_live_duration_gift_mq_flink)
10. 活动风控计算 (activity_risk_control)

## 故障排除

### API连接问题

- 检查API地址是否正确
- 确认用户名和密码是否有效
- 验证网络连接

### 没有FINISHED状态的作业

- 检查Flink集群中是否有已完成的作业
- 如果需要处理其他状态的作业，可以修改脚本中的筛选条件

### 无法获取savepoint路径

- 确认作业是否有创建savepoint
- 检查checkpoint API的响应

### 解析配置失败

- 检查作业配置中是否包含marketing.ddl和marketing.sql
- 检查这些字段的格式是否符合预期
