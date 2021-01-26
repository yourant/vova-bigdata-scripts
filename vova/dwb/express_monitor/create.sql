drop table dwb.dwb_vova_logistics_order_monitor
CREATE TABLE dwb.dwb_vova_logistics_order_monitor
(
    cur_date                    STRING COMMENT 'd_日期',
    region_code                 STRING COMMENT 'd_国家',
    collect_type                STRING COMMENT 'd_',
    logistics_cnt               STRING COMMENT 'i_集运单数',
    logistics_cnt_rate          STRING COMMENT 'i_渗透率',
    four_day_pick_up_rate       STRING COMMENT 'i_4天上线率',
    seven_day_in_warehouse_rate STRING COMMENT 'i_7天入库率',
    nine_day_out_warehouse_rate STRING COMMENT 'i_9天出库率',
    twelven_day_refund_rate     STRING COMMENT 'i_12天取消率',
    test                        STRING
)
    COMMENT '集运国内段监控'
    PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;