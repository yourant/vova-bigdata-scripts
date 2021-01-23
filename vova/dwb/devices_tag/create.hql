用户分层:
1.新老用户
2.r值划分表现
3.m值划分表现
4.f值划分表现
5.新老用户下单率

drop table dwb.dwb_vova_devices_tag;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_devices_tag
(
    datasource             string,
    event_date             date,
    region_code            string,
    main_channel           string,
    first_pay_uv           bigint,
    pay_uv                 bigint,
    dau                    bigint,
    dau_no_pay             bigint,
    dau_continue_1d        bigint,
    dau_continue_1d_no_pay bigint,
    loss_user_1            bigint,
    loss_user_1_pay        bigint,
    loss_user_2            bigint,
    loss_user_2_pay        bigint,
    dau_r1                 bigint,
    dau_r1_continue_1d     bigint,
    dau_r2                 bigint,
    dau_r2_continue_1d     bigint,
    dau_r3                 bigint,
    dau_r3_continue_1d     bigint,
    dau_r4                 bigint,
    dau_r4_continue_1d     bigint,
    dau_m1                 bigint,
    dau_m1_continue_1d     bigint,
    dau_m2                 bigint,
    dau_m2_continue_1d     bigint,
    dau_m3                 bigint,
    dau_m3_continue_1d     bigint,
    dau_m4                 bigint,
    dau_m4_continue_1d     bigint,
    dau_F1                 bigint,
    dau_F1_continue_1d     bigint,
    dau_F2                 bigint,
    dau_F2_continue_1d     bigint,
    dau_F3                 bigint,
    dau_F3_continue_1d     bigint,
    dau_F4                 bigint,
    dau_F4_continue_1d      bigint
) COMMENT '用户分层' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_devices_tag/"
;

用户分层
2021-01-23 历史数据迁移
dwb.dwb_vova_devices_tag

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_devices_tag/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_devices_tag/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_devices_tag/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_devices_tag/*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_devices_tag/  s3://bigdata-offline/warehouse/dwb/dwb_vova_devices_tag

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_devices_tag/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_devices_tag/

msck repair table dwb.dwb_vova_devices_tag;
select * from dwb.dwb_vova_devices_tag limit 20;




