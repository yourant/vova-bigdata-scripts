CREATE EXTERNAL TABLE IF NOT EXISTS tmp.dwb_fd_ads_cost
(
    event_date      date COMMENT 'event_date',
    project_name      string COMMENT 'datasource',
    country_code     string COMMENT 'region_code',
    ga_channel      string COMMENT 'ga_channel',
    platform        string COMMENT 'platform',
    activate_dau    bigint COMMENT '当天激活用户数',
    tot_gmv         decimal(20, 2) COMMENT 'gmv',
    tot_cost        decimal(20, 2) COMMENT '当天广告花费',
    tot_country_gmv  decimal(20, 2) COMMENT '分国家不分渠道platform 的gmv',
    tot_country_cost decimal(20, 2) COMMENT '分国家不分渠道platform 的cost',
    gmv_7d          decimal(20, 2) COMMENT '210天前-180天前时间段30天每天近7天激活用户gmv',
    gmv_180d        decimal(20, 2) COMMENT '210天前-180天前时间段30天每天近180天激活用户gmv',
    est_gmv_7d      decimal(20, 2) COMMENT '预估广告花费'
) COMMENT 'dwb_fd_ads_cost' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;