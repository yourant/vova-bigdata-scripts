drop table dwd.dwd_vova_fact_push_click;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_vova_fact_push_click
(
    --app_event_log_message_push
    datasource       string comment '数据平台',
    buyer_id         bigint comment '买家ID',
    r_tag            string,
    f_tag            string,
    m_tag            string,
    is_new           string,
    task_id          bigint,
    click_time       timestamp,
    from_domain      string,
    platform         string,
    device_id        string,
    app_version      string,
    region_code      string,
    currency_code    string,
    -- app_push_task
    config_id        bigint,
    push_mode_id     string,
    push_platform_id string,
    priority         string,
    message_title    string,
    message_body     string,
    target_link      string,
    task_type        string,
    target_type      string,
    target_tags      string,
    time_zone        string,
    push_region_code string,
    task_status      string,
    push_time        timestamp
) COMMENT '推送点击事实表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;