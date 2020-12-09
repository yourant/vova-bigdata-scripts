CREATE TABLE IF NOT EXISTS dwb.dwb_fd_snowplow_uv_dau
(
    project            STRING comment '网站名称',
    platform_type      STRING comment '平台',
    dau                BIGINT comment 'dau',
    uv                 BIGINT comment 'uv'

) comment 'snowplow uv dau'
    PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS ORC;