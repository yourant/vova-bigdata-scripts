create table if not exists dwd.dwd_fd_user_repurchase_weekly
(
    current_week      string comment '统计时间-周，第一次出现在第几周-存放的是每周的第一天',
    user_id           bigint comment '用户ID',
    order_id          bigint comment '订单',
    project           string comment '组织',
    country_code      string comment '国家',
    platform_type     string comment '平台',
    ga_channel        string comment '用户渠道来源',
    user_is_first_pay string comment '是否首次支付用户',
    user_is_first_reg string comment '是否首次注册用户'
) comment '周复购率，UTC时间'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


create table if not exists dwb.dwb_fd_user_repurchase_weekly
(
    project string comment '组织',
    country_code string comment '国家',
    platform_type string comment '平台',
    user_is_first_pay string comment '是否首次支付用户 yes/no',
    user_is_first_reg string comment '是否首次注册用户 yes/no',
    ga_channel string comment '用户渠道来源',
    purchase_current_week int comment '当月支付用户数',
    p1w int comment '次1周复购用户数',
    p2w int comment '次2周复购用户数',
    p3w int comment '次3周复购用户数',
    p4w int comment '次4周复购用户数',
    p5w int comment '次5周复购用户数',
    p6w int comment '次6周复购用户数',
    p7w int comment '次7周复购用户数',
    p8w int comment '次8周复购用户数',
    p1w_rate decimal(15,4) COMMENT '次1周复购率',
    p2w_rate decimal(15,4) COMMENT '次2周复购率',
    p3w_rate decimal(15,4) COMMENT '次3周复购率',
    p4w_rate decimal(15,4) COMMENT '次4周复购率',
    p5w_rate decimal(15,4) COMMENT '次5周复购率',
    p6w_rate decimal(15,4) COMMENT '次6周复购率',
    p7w_rate decimal(15,4) COMMENT '次7周复购率',
    p8w_rate decimal(15,4) COMMENT '次8周复购率'
)comment '用户复购周留存表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC;