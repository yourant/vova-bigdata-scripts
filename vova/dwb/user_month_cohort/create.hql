app月度留存:

app月度留存           table   dwb.dwb_vova_user_month_cohort
首次支付用户月度留存    table   dwb.dwb_vova_order_month_start_up_cohort
用户复购月度留存       table   dwb.dwb_vova_order_month_cohort



#用户月度留存
drop table dwb.dwb_vova_user_month_cohort;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_user_month_cohort
(
    start_month  date COMMENT '启动日期',
    datasource   STRING,
    region_code  STRING,
    platform     STRING,
    main_channel STRING,
    next_0_num   bigint,
    next_1_num   bigint,
    next_2_num   bigint,
    next_3_num   bigint,
    next_4_num   bigint,
    next_5_num   bigint,
    next_6_num   bigint,
    next_7_num   bigint,
    next_8_num   bigint,
    next_9_num   bigint,
    next_10_num  bigint,
    next_11_num  bigint,
    next_12_num  bigint,
    buyer_type  string COMMENT '买家类型：only_pre_direct(只购买过前置仓直发),include_pre_direct(购买过前置仓直发),no_pre_direct(未购买过前置仓直发)'
) COMMENT '用户留存报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_user_month_cohort/"
;

drop table dwb.dwb_vova_order_month_cohort;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_order_month_cohort
(
    pay_month   date,
    next_0_num  bigint,
    next_1_num  bigint,
    next_2_num  bigint,
    next_3_num  bigint,
    next_4_num  bigint,
    next_5_num  bigint,
    next_6_num  bigint,
    next_7_num  bigint,
    next_8_num  bigint,
    next_9_num  bigint,
    next_10_num  bigint,
    next_11_num  bigint,
    next_12_num  bigint,
    is_new_user string COMMENT '是否新用户',
    region_code string COMMENT 'region_code',
    platform string COMMENT 'platform',
    datasource string,
    buyer_type string COMMENT '买家类型：only_pre_direct(只购买过前置仓直发),include_pre_direct(购买过前置仓直发),no_pre_direct(未购买过前置仓直发)'
) COMMENT '用户复购月度报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_order_month_cohort/"
;


drop table dwb.dwb_vova_order_month_start_up_cohort;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_order_month_start_up_cohort
(
    pay_month   date,
    datasource   string,
    region_code   string,
    platform   string,
    next_0_num  bigint,
    next_1_num  bigint,
    next_2_num  bigint,
    next_3_num  bigint,
    next_4_num  bigint,
    next_5_num  bigint,
    next_6_num  bigint,
    next_7_num  bigint,
    next_8_num  bigint,
    next_9_num  bigint,
    next_10_num  bigint,
    next_11_num  bigint,
    next_12_num  bigint,
    buyer_type string COMMENT '买家类型：only_pre_direct(只购买过前置仓直发),include_pre_direct(购买过前置仓直发),no_pre_direct(未购买过前置仓直发)'
) COMMENT '用户复购月度报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_order_month_start_up_cohort/"
;











