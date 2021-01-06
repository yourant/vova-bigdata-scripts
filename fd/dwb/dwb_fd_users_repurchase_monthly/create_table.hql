create table if not exists dwd.dwd_fd_user_repurchase_monthly
(
    current_month     string comment '统计时间-周，第一次出现在第几周-存放的是每周的第一天',
    user_id           bigint comment '用户ID',
    order_id          bigint comment '订单',
    project           string comment '组织',
    country_code      string comment '国家',
    platform_type     string comment '平台',
    ga_channel        string comment '用户渠道来源',
    user_is_first_pay string comment '是否首次支付用户',
    user_is_first_reg string comment '是否首次注册用户'
) comment '月复购率，UTC时间'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


create table if not exists dwb.dwb_fd_user_repurchase_monthly
(
    current_month string comment '月份，统计当月的第一天',
    project string comment '组织',
    country_code string comment '国家',
    platform_type string comment '平台',
    user_is_first_pay string comment '是否首次支付用户 yes/no',
    user_is_first_reg string comment '是否首次注册用户 yes/no',
    ga_channel string comment '用户渠道来源',
    purchase_current_month int comment '当月支付用户数',
    p1m int comment '次1月复购用户数',
    p2m int comment '次2月复购用户数',
    p3m int comment '次3月复购用户数',
    p4m int comment '次4月复购用户数',
    p5m int comment '次5月复购用户数',
    p6m int comment '次6月复购用户数',
    p7m int comment '次7月复购用户数',
    p8m int comment '次8月复购用户数',
    p9m int comment '次9月复购用户数',
    p10m int comment '次10月复购用户数',
    p11m int comment '次11月复购用户数',
    p12m int comment '次12月复购用户数',
    p1m_rate decimal(15,4) COMMENT '次1月复购率',
    p2m_rate decimal(15,4) COMMENT '次2月复购率',
    p3m_rate decimal(15,4) COMMENT '次3月复购率',
    p4m_rate decimal(15,4) COMMENT '次4月复购率',
    p5m_rate decimal(15,4) COMMENT '次5月复购率',
    p6m_rate decimal(15,4) COMMENT '次6月复购率',
    p7m_rate decimal(15,4) COMMENT '次7月复购率',
    p8m_rate decimal(15,4) COMMENT '次8月复购率',
    p9m_rate decimal(15,4) COMMENT '次9月复购率',
    p10m_rate decimal(15,4) COMMENT '次10月复购率',
    p11m_rate decimal(15,4) COMMENT '次11月复购率',
    p12m_rate decimal(15,4) COMMENT '次12月复购率'
)comment '用户复购月留存表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC;