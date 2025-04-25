# Flink作业批量启动工具使用指南

本工具可以批量启动Flink作业，通过读取处理后的作业配置文件，使用指定的公共JAR（common JAR）来启动SQL作业。支持启动全部作业或指定作业ID，并可设置启动间隔时间，避免对集群造成过大压力。

## 功能特点

- 从配置文件（默认`output/processed_jobs.json`）中加载作业配置
- 使用统一的公共JAR ID（common JAR）来启动所有SQL作业
- 支持启动所有作业或指定特定作业
- 可设置每个作业启动后的等待间隔时间（默认5秒）
- 支持干运行模式(dry-run)，预览将要执行的操作而不实际执行
- 完整的错误处理和日志记录
- 自动重试机制，确保API请求的可靠性

## 使用方法

### 基本用法

启动配置文件中的所有作业：

```bash
python flink_job_starter.py --base-url https://flink.xxx.com --username admin --password admin --jar-id c21c2c24-3941-42aa-8bed-2c34d7d77670_xxx-marketing-flink-MarketingSQLTask-common-prod-0827022334.jar --all
```

启动指定的一个或多个作业：

```bash
python flink_job_starter.py --job-id 527cd5269ddbf9a83fc238fb9b7f4412-new_user_tag_task,b18d18538ef206192f7764c48c34a5a1-last_list_tag
```

使用干运行模式，只显示将要执行的操作，不实际启动作业：

```bash
python flink_job_starter.py --all --base-url http://127.0.0.1:8081 --jar-id f7b40832-d290-4c8c-876b-17e92567dd0a_xxx-common-test-20240419.jar --dry-run
```

### 完整参数说明

```
参数:
  --config-file CONFIG_FILE  作业配置文件路径 (默认: output/processed_jobs.json)
  --job-id JOB_ID           指定要启动的作业ID，多个ID用逗号分隔
  --all                     启动配置文件中的所有作业
  --dry-run                 不实际启动作业，只打印要执行的操作
  --base-url BASE_URL       Flink API基地址 (默认: http://127.0.0.1:8081)
  --username USERNAME       API认证用户名 (默认: admin)
  --password PASSWORD       API认证密码 (默认: admin)
  --jar-id JAR_ID           公共JAR文件ID (默认: f7b40832-d290-4c8c-876b-17e92567dd0a_xxx-common-test-20240419.jar)
  --interval INTERVAL       启动作业之间的间隔时间(秒) (默认: 5)
```

### 示例用法

1. 启动所有作业，每次启动后等待10秒：

```bash
python flink_job_starter.py --all --interval 10
```

2. 使用指定的JAR ID启动作业：

```bash
python flink_job_starter.py --all --jar-id abc123_xxx-common-prod-20250410.jar
```

3. 从自定义配置文件启动作业：

```bash
python flink_job_starter.py --all --config-file custom_jobs.json
```

4. 使用自定义认证信息启动作业：

```bash
python flink_job_starter.py --all --username admin --password secret123
```

5. 先预览要启动的作业，然后再实际执行：

```bash
# 先预览
python flink_job_starter.py --all --dry-run

# 确认无误后执行
python flink_job_starter.py --all
```

## 工作流程

1. 连接到Flink API
2. 从配置文件加载作业配置
3. 如果指定了`--job-id`参数，筛选出指定的作业配置
4. 依次对每个作业执行启动操作：
   - 向JAR的run API端点发送POST请求，包含作业配置作为请求体
   - 等待作业启动响应
   - 如果有下一个作业，等待指定的间隔时间
5. 汇总并输出启动结果

## 最佳实践

- 在生产环境中执行前，先使用`--dry-run`参数预览操作
- 为避免集群负载突增，根据作业复杂度合理设置`--interval`参数
- 批量启动前，确保集群有足够的资源处理这些作业
- 使用正确的共享JAR ID，特别是在不同环境（开发、测试、生产）之间切换时
- 确保作业配置文件包含正确的配置，可以先处理单个作业进行测试
- 对于重要的作业，建议保存每次部署的配置和日志，以便回滚

## 配置文件格式

工具所需的配置文件格式如下（通常由`flink_job_processor.py`生成）：

```json
{
  "527cd5269ddbf9a83fc238fb9b7f4412-new_user_tag_task": {
    "entryClass": "com.quick.marketing.MarketingSQLTask",
    "parallelism": null,
    "programArgs": "eyJtYXJrZXRpbmcuZGRsIjogWyJDUk...(Base64编码的配置)...",
    "savepointPath": "s3://xxx/flink/savepoint-527cd5-f3f0c726b021"
  },
  "b18d18538ef206192f7764c48c34a5a1-last_list_tag": {
    "entryClass": "com.quick.marketing.MarketingSQLTask",
    "parallelism": null,
    "programArgs": "eyJtYXJrZXRpbmcuZGRsIjogWyJDUk...(Base64编码的配置)...",
    "savepointPath": "s3://xxx/flink/savepoint-b18d18-92a02953e875"
  }
}
```

## 注意事项

- 启动大量作业可能会对Flink JobManager造成负载
- API认证信息需要具有启动作业的权限
- 确保使用正确的JAR ID，错误的JAR ID会导致启动失败
- 由于启动操作需要加载恢复点，可能会消耗较多资源
- 部署前确认配置文件中的savepointPath是否有效
- 确保savepointPath是可访问的，被启动作业需要权限访问此路径
- 对于ETL作业，应使用专门的ETL启动流程，而不是此工具

## 故障排除

- 如果认证失败，检查用户名和密码是否正确
- 如果找不到JAR ID，确认ID是否正确并检查是否已上传到集群
- 如果作业启动失败，检查savepoint路径是否有效
- 如果集群资源不足，尝试增加启动间隔或减少并行启动的作业数
- 如果配置文件不正确，重新使用`flink_job_processor.py`生成
- 检查日志中的具体错误信息，通常会包含问题的详细说明
