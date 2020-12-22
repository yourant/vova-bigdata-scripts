create table if not exists dwd.dwd_fd_users_repurchase_weekly
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
row format delimited fields terminated by '\t' lines terminated by '\n'
stored as orc;