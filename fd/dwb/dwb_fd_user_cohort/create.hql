DROP TABLE dwd.dwd_fd_domain_channel;
CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_fd_domain_channel
(
    domain_userid string COMMENT 'domain_userid',
    ga_channel    string COMMENT 'ga_channel'
) COMMENT 'dwd_fd_domain_channel'
    STORED AS PARQUETFILE;


DROP TABLE dwb.dwb_fd_user_cohort;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_fd_user_cohort
(
    event_date    date COMMENT 'event_date',
    project       string COMMENT 'datasource',
    country       string COMMENT 'region_code',
    platform_type string COMMENT 'platform',
    is_new_user   string COMMENT 'is_new',
    ga_channel    string COMMENT 'ga_channel',
    dau           bigint,
    next_1_num    bigint,
    next_3_num    bigint,
    next_7_num    bigint,
    next_15_num   bigint,
    next_30_num   bigint
) COMMENT 'dwb_fd_user_cohort' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;
