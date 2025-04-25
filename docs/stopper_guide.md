# Flink作业批量停止工具使用指南

本工具可以批量停止Flink集群中运行的作业，支持全部停止或指定作业ID停止，并可设置停止间隔时间，每次停止后等待指定秒数再继续下一个作业，避免对集群造成过大压力。

## 功能特点

- 支持停止所有RUNNING状态的作业
- 支持指定作业ID进行停止
- 可设置每个作业停止后的等待间隔时间（默认15秒）
- 支持干运行模式(dry-run)，预览将要执行的操作而不实际执行
- 完整的错误处理和日志记录
- 自动重试机制，确保API请求的可靠性

## 使用方法

### 基本用法

停止所有正在运行的作业：

```bash
python flink_job_stopper.py --all
```

停止指定的一个或多个作业：

```bash
python flink_job_stopper.py --job-id ce38fe58413a61c4b2250594ff816bb9,4b2757789b9d3494d4757214d48d4df9
```

使用干运行模式，只显示将要执行的操作，不实际停止作业：

```bash
python flink_job_stopper.py --all --base-url https://flink.xxx.com --username admin --password admin  --job-id 6e74238c4e4c931445c3dfb70509ff6b




python flink_job_stopper.py --all --base-url https://flink.xxx.com --username admin --password admin --dry-run
```

### 完整参数说明

```
参数:
  --job-id JOB_ID       指定要停止的作业ID，多个ID用逗号分隔
  --all                 停止所有RUNNING状态的作业
  --dry-run             不实际停止作业，只打印要执行的操作
  --base-url BASE_URL   Flink API基地址 (默认: http://127.0.0.1:8081)
  --username USERNAME   API认证用户名 (默认: admin)
  --password PASSWORD   API认证密码 (默认: admin)
  --interval INTERVAL   停止作业之间的间隔时间(秒) (默认: 15)
```

### 示例用法

1. 停止所有运行中的作业，每次停止后等待30秒：

```bash
python flink_job_stopper.py --all --interval 30
```

2. 使用自定义认证信息停止指定作业：

```bash
python flink_job_stopper.py --job-id ce38fe58413a61c4b2250594ff816bb9 --username admin --password secret123
```

3. 先预览要停止的作业，然后再实际执行：

```bash
# 先预览
python flink_job_stopper.py --all --dry-run

# 确认无误后执行
python flink_job_stopper.py --all
```

## 工作流程

1. 连接到Flink API
2. 如果指定了`--all`参数，获取所有RUNNING状态的作业列表
3. 如果指定了`--job-id`参数，使用提供的作业ID列表
4. 依次对每个作业执行停止操作：
   - 向作业的stop API端点发送POST请求
   - 等待作业停止响应
   - 如果有下一个作业，等待指定的间隔时间
5. 汇总并输出停止结果

## 最佳实践

- 在生产环境中执行前，先使用`--dry-run`参数预览操作
- 为避免集群负载突增，合理设置`--interval`参数
- 对于关键业务作业，建议单独处理而不是批量操作
- 作业停止前，确保已有相应的保存点(savepoint)或检查点(checkpoint)
- 建议在低峰期执行批量停止操作

## 注意事项

- 停止作业是不可逆操作，一旦停止需重新提交才能恢复
- 在没有保存点的情况下停止作业可能会导致数据丢失
- 停止大量作业可能对Flink JobManager造成负载
- API认证信息需要具有停止作业的权限
- 如果作业配置了终止时保存状态(terminate-with-savepoint)，停止过程可能较慢

## 故障排除

- 如果认证失败，检查用户名和密码是否正确
- 如果API连接超时，可能是网络问题或Flink集群负载过高
- 如果特定作业无法停止，可能是该作业已经处于终止状态
- 集群配置可能会限制并发操作，如果遇到错误请增加间隔时间
