#### ETL

>1. 账户数据源 xxxetl_marketing_job | xxx-etl-marketing-flink-xxxETLMarketingTask-1.0-SNAPSHOT-prod-0410084144.jar
>2. TP数据源 xxxpattern_etl_marketing_job | xxxpattern_etl_marketing_job-prod-0410082231.jar
>
>3. game1数据源 ta_game_job_1 | ta-game-job1-6.1-SNAPSHOT-prod-0328024033.jar
>
>4. game2数据源 ta_game_job_2 | xxx-ta-Game-xxxdb.jar
>
>5. 麦位数据源 xxxmic_etl_kafka_job | xxx-etl-mic-1.0-SNAPSHOT-prod-0115024411.jar
>
>6. 主播数据源 ta_talent_etl_job | ta_talent_etl_job.jar
>
>7. 用户数据源 ta_user_behavior_job | xxx-ta-user_behavior-4.0-SNAPSHOT-prod-0324100437.jar
>
>8. 用户数据源 ta_analysis_job | ta_analysis_user_job.jar
>
>9. 订单充值ta_analysis_order_job | ta_analysis_order_job.jar
>
>10. 任务数据源（老的任务系统同步任务）ta_mission_job | ta_mission_job-prod-0410081726.jar
>
>11. 任务数据源（营销平台任务完成同步）sem_user_mission_job | sem_user_mission_job-prod-0410101450.jar
>
>12. 活动风控计算 activity_risk_control | ActivityInspect-1.0-SNAPSHOT_0410163844.jar
>
>13. ta_history_rate_job | ta_history_rate_job-prod-0410081513.jar
>
>14. ta_active_user_job | ta_active_user_info-prod-0410081108.jar
>
>15. oyelite_ta_reel_operation_job | oyelite-reel-1.0-SNAPSHOT-prod-0410085720.jar
>
>16. 工资单 talent_live_duration_gift_mq_flink | xxx-salary-flink-SalaryJob-1.0.0_0410103214.jar



#### 对应关系

| Job Name                                                     | 分支                                                         | jar                        | 用途                                          |
| ------------------------------------------------------------ | ------------------------------------------------------------ | -------------------------- | --------------------------------------------- |
| ta_game_job拆分成ta_game_job_1(ludo和tpgo相关)和ta_game_job_2(pattern,cq,gr,gd等) | feature/game_status_branch1和feature/game_xxxdb_branch2分支 | 活动                       | game数据源                                    |
| xxxmic_etl_kakfa_job                                    | xxx-etl-marketing-flink/mic分支                          | 活动                       | 麦位数据源                                    |
| ta_analysis_job拆分成ta_analysis_user_job任务和ta_analysis_user_job任务 | xxx-ta/ta-zyx分支(t_user表)和feature/ta_pay-order分支(t_order表) | 充值 看生产数数是否正常    | 用户数据源/order充值（同步t_user和t_order表） |
| xxxetl_marketing_job                                    | xxx-etl-marketing-flink/init_version分支                 | 活动                       | 账户数据源（同步operation表）                 |
| ta_user_behavior_job                                         | xxx-ta/feature/user_info分支                             | 新人任务打标签  所有的标签 | 用户数据源（同步user_extend表)                |
| xxxpattern_etl_marketing_job                            | xxx-etl-marketing-flink/in_teen_patti分支                | 活动                       | TP数据源                                      |
| ta_history_rate_job                                          | xxx-ta/feature/ta2.0-zyx分支                             | 数数                       | 汇率表同步                                    |
| ta_mission_job                                               | xxx-ta/feature/newbie2.0-zyx分支                         | 数数                       | 任务数据源（老的任务系统同步任务）            |
| sem_user_mission_job                                         | xxx-ta/feature/v2.7.5-zyx分支                            | 数数                       | 任务数据源（营销平台任务完成同步）            |
| ta_active_user_info                                          | xxx-ta/ta3.0-zyx分支                                     | 数数                       | 活跃用户表同步                                |
| ta_talent_etl_job                                            | xxx-ta/feature/talent_info-zyx                           | 任务                       | 官方主播数据同步                              |
| talent_live_duration_gift_mq_flink                           | xxx-salary-flink/salary_1.2                              | 工资单                     | 工资单                                        |







