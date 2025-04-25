# Flink作业转换工具使用指南

## 功能概述

Flink作业转换工具用于将从Flink REST API提取的原始作业配置格式转换为新的目标格式，主要功能包括：

1. 自动获取Flink API中FINISHED状态的作业列表
2. 获取作业的checkpoint/savepoint信息
3. 将原有作业配置转换为指定的新格式，包括：
   - 提取marketing.ddl和marketing.sql
   - 使用Base64编码这些配置
   - 按照指定结构构建新的配置对象

## 系统要求

- Python 3.6+
- 网络连接到Flink REST API服务器

## 安装

1. 确保您已安装Python 3.6或更高版本
2. 安装所需的依赖库：
```bash
pip install requests
```

## 使用方法

### 基本用法

```bash
python flink_job_transformer.py --input <输入文件路径> --output <输出文件路径> --base-url <Flink API地址>
```

### 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--input` | 输入文件路径，包含原始作业配置 | (必填) |
| `--output` | 输出文件路径 | input路径基础上添加.transformed后缀 |
| `--base-url` | Flink API基地址 | http://127.0.0.1:8081 |
| `--username` | API认证用户名 | flink |
| `--password` | API认证密码 | flink-!@xxx2021 |

### 示例

基本用法：
```bash
python flink_job_transformer.py --input examples/sample_input.json --output output/transformed_jobs.json
```

指定不同的API地址：
```bash
python flink_job_transformer.py --input examples/sample_input.json --base-url http://flink-api.example.com:8081
```

自定义认证信息：
```bash
python flink_job_transformer.py --input examples/sample_input.json --username admin --password secret123
```

## 输入格式

输入文件是一个JSON文件，包含从Flink REST API提取的作业配置。通常是通过flink-auto工具的main.py生成的，格式示例：

```json
{
  "4b2757789b9d3494d4757214d48d4df9": {
    "marketing.ddl": [
      "CREATE TABLE kafka_source (...) WITH(...)",
      "CREATE TABLE business_operation_events_sink (...) WITH(...)"
    ],
    "marketing.sql": [
      "INSERT INTO business_operation_events_model_sink SELECT ...",
      "INSERT INTO business_operation_events_sink SELECT ..."
    ],
    "pipeline.name": "competition_20250414_ts_task"
  },
  "b18d18538ef206192f7764c48c34a5a1": {
    "marketing.ddl": [...],
    "marketing.sql": [...],
    "pipeline.name": "..."
  }
}
```

## 输出格式

输出文件是一个JSON文件，包含转换后的作业配置，格式示例：

```json
{
  "b18d18538ef206192f7764c48c34a5a1": {
    "entryClass": "com.quick.marketing.MarketingSQLTask",
    "parallelism": null,
    "programArgs": "ewogICAgIm1hcmtldGluZy5kZG...(Base64编码的原始配置)",
    "savepointPath": "s3://xxx/flink/savepoint-b18d18-92a02953e875"
  }
}
```

转换后的结构包含：
- `entryClass`: 固定值 "com.quick.marketing.MarketingSQLTask"
- `parallelism`: null
- `programArgs`: Base64编码的原始作业配置
- `savepointPath`: 如果可以获取，则包含savepoint路径

## 工作流程

1. 读取输入文件中的原始作业配置
2. 通过Flink API查询FINISHED状态的作业列表
3. 对每个处于FINISHED状态的作业：
   - 获取其checkpoint信息，特别是savepoint路径
   - 提取marketing.ddl、marketing.sql和pipeline.name
   - 将这些配置进行Base64编码
   - 构建新的配置对象，包含entryClass、parallelism、programArgs和savepointPath
4. 将转换后的所有作业配置保存到输出文件

## 常见问题与解决方案

### API连接问题

**问题**: 无法连接到Flink API服务器
**解决方案**: 
- 检查API地址和端口是否正确
- 确认网络连接可用
- 验证认证信息是否正确

### 未找到FINISHED状态的作业

**问题**: 工具没有找到任何FINISHED状态的作业
**解决方案**:
- 确认有作业处于FINISHED状态
- 检查API返回的作业状态

### 未找到savepoint信息

**问题**: 转换后的配置中没有savepointPath
**解决方案**:
- 确认作业有保存savepoint
- 检查checkpoint API是否返回savepoint信息

## 日志信息

工具运行时会输出详细的日志信息，帮助定位问题：

- `[INFO]` - 一般信息，如进度、成功消息等
- `[WARN]` - 警告信息，如未找到savepoint
- `[ERROR]` - 错误信息，如API连接失败

## 示例输出

成功运行示例：
```
=== Flink作业转换工具 ===
[INFO] 初始化转换工具，API地址: http://127.0.0.1:8081
[INFO] 从 examples/sample_input.json 读取原始配置...
[INFO] 读取到 2 个作业配置
[INFO] 获取状态为FINISHED的作业列表...
[INFO] 找到 1 个状态为FINISHED的作业(共 5 个作业)
[INFO] 处理作业 4b2757789b9d3494d4757214d48d4df9...
[WARN] 作业 4b2757789b9d3494d4757214d48d4df9 不是FINISHED状态，跳过
[INFO] 处理作业 b18d18538ef206192f7764c48c34a5a1...
[INFO] 作业 b18d18538ef206192f7764c48c34a5a1 处于FINISHED状态，获取checkpoint信息...
[INFO] 获取作业 b18d18538ef206192f7764c48c34a5a1 的checkpoint信息...
[INFO] 成功获取savepoint路径: s3://xxx/flink/savepoint-b18d18-92a02953e875
[INFO] 转换作业配置...
[INFO] 作业 b18d18538ef206192f7764c48c34a5a1 转换完成
[INFO] 将转换后的配置保存到 output/transformed_jobs.json...
[INFO] 成功处理 1 个作业配置
=== 转换完成，共成功处理 1 个作业配置 ===
