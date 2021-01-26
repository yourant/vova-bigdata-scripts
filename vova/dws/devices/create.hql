drop table dws.dws_vova_devices;
CREATE EXTERNAL TABLE IF NOT EXISTS dws.dws_vova_devices
(
    datasource             string,
    device_id              string COMMENT 'device_id',
    first_pay_time         timestamp COMMENT '首单支付时间',
    last_pay_time          timestamp COMMENT '最后一单支付时间',
    last_1_pay_time        timestamp COMMENT '倒数第二单支付时间',
    pay_gmv                bigint COMMENT '支付总的gmv',
    pay_order              bigint COMMENT '支付单数',
    last_start_up_date     timestamp COMMENT '最后一次启动日期',
    loss_user              string COMMENT '查询日期前推两个月登陆的dau，在接下来 两个月内未启动app的用户|自四月一日起至查询日期前两个月内启动APP的所有用户，在查询日期日期前两个月未启动APP的用户',
    is_refund              bigint COMMENT '是否退过款',
    is_not_logistic_refund bigint COMMENT '非物流退款',
    R_tag                  string COMMENT '最近两次支付间隔',
    F_tag                  string COMMENT '支付频次',
    M_tag                  string COMMENT '均价',
    continue_1d            string comment '连续两天登录'
) COMMENT '设备特殊标签'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



