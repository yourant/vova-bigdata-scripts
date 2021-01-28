drop table if exists dwb.dwb_vova_collection_link;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_collection_link
(
    weekday            string COMMENT 'd_日期',
    region_code        string COMMENT 'd_国家',
    carrier_name       string COMMENT 'd_尾程物流渠道',
    duan_to_duan       decimal(10, 2) comment 'i_端到端时效',
    duan_to_duan_90    decimal(10, 2) comment 'i_端到端90分位时效',
    send_exp           decimal(10, 2) comment 'i_发货平均时效',
    send_exp_90        decimal(10, 2) comment 'i_发货90分位时效',
    head_get_exp       decimal(10, 2) comment 'i_揽收平均时效头程',
    head_get_exp_90    decimal(10, 2) comment 'i_头程90分位时效',
    stock_in           decimal(10, 2) comment 'i_仓内平均时效',
    stock_in_90        decimal(10, 2) comment 'i_仓内90分位时效',
    in_stock           decimal(10, 2) comment 'i_入库平均时效',
    in_stock_90        decimal(10, 2) comment 'i_入库90分位时效',
    out_stock          decimal(10, 2) comment 'i_出库平均时效',
    out_stock_90       decimal(10, 2) comment 'i_出库90分位时效',
    stay_stock         decimal(10, 2) comment 'i_在库等待平均时长',
    stay_stock_90      decimal(10, 2) comment 'i_在库等待90分位时长',
    tail_main_route    decimal(10, 2) comment 'i_尾程干线平均时效',
    tail_main_route_90 decimal(10, 2) comment 'i_尾程干线90分位时效',
    last_time          decimal(10, 2) comment 'i_末端平均时效',
    last_time_90       decimal(10, 2) comment 'i_末端90分位时效'
) COMMENT '集运订单全链路时效监控'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



