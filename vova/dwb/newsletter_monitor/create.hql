DROP TABLE IF EXISTS dwb.dwb_vova_newsletter_monitor_bounce;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_newsletter_monitor_bounce(
    `app_name`                    string                 COMMENT 'd_app_name',
    `bounce_rate_max`             decimal(13,2)          COMMENT 'i_最大bounce率',
    `bounce_rate_avg`             decimal(13,2)          COMMENT 'i_平均bounce率'
)COMMENT 'newsletter bounce率监控' PARTITIONED BY (`pt` STRING) STORED AS PARQUETFILE;

DROP TABLE IF EXISTS dwb.dwb_vova_newsletter_monitor_task;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_newsletter_monitor_task(
    `id`                          int                    COMMENT 'd_id',
    `app_name`                    string                 COMMENT 'i_app_name',
    `task_name`                   string                 COMMENT 'i_名称',
    `pt`                          timestamp              COMMENT 'i_发送时间',
    `succ_num`                    int                    COMMENT 'i_发送成功量',
    `gmv_1d`                      decimal(13,2)          COMMENT 'i_邮件发送成功用户一日内gmv',
    `rate`                        decimal(13,2)          COMMENT 'i_1日gmv/发送成功量'
)COMMENT 'newsletter任务监控' STORED AS PARQUETFILE;
