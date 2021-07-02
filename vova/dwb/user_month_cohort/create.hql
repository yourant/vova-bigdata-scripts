app月度留存:

table   dwb.dwb_vova_user_month_cohort              app月度留存
table   dwb.dwb_vova_order_month_start_up_cohort    首次支付用户月度留存
table   dwb.dwb_vova_order_month_cohort             用户复购月度留存


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
    next_13_num  bigint,
    next_14_num  bigint,
    next_15_num  bigint,
    next_16_num  bigint,
    next_17_num  bigint,
    next_18_num  bigint,
    next_19_num  bigint,
    next_20_num  bigint,
    next_21_num  bigint,
    next_22_num  bigint,
    next_23_num  bigint,
    next_24_num  bigint,
    -- buyer_type  string COMMENT '买家类型：only_pre_direct(只购买过前置仓直发),include_pre_direct(购买过前置仓直发),no_pre_direct(未购买过前置仓直发)',
    buyer_type       string comment '用户是否当月新激活:all、Y、N'
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
    next_13_num  bigint,
    next_14_num  bigint,
    next_15_num  bigint,
    next_16_num  bigint,
    next_17_num  bigint,
    next_18_num  bigint,
    next_19_num  bigint,
    next_20_num  bigint,
    next_21_num  bigint,
    next_22_num  bigint,
    next_23_num  bigint,
    next_24_num  bigint,
    is_new_user string COMMENT '是否新用户',
    region_code string COMMENT 'region_code',
    platform    string COMMENT 'platform',
    datasource  string,
    -- buyer_type  string COMMENT '买家类型：only_pre_direct(只购买过前置仓直发),include_pre_direct(购买过前置仓直发),no_pre_direct(未购买过前置仓直发)',
    buyer_type      string comment '用户是否当月新激活:all、Y、N'
) COMMENT '用户复购月度报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_order_month_cohort/"
;


drop table dwb.dwb_vova_order_month_start_up_cohort;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_order_month_start_up_cohort
(
    pay_month   date,
    datasource  string,
    region_code string,
    platform    string,
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
    next_10_num bigint,
    next_11_num bigint,
    next_12_num bigint,
    next_13_num  bigint,
    next_14_num  bigint,
    next_15_num  bigint,
    next_16_num  bigint,
    next_17_num  bigint,
    next_18_num  bigint,
    next_19_num  bigint,
    next_20_num  bigint,
    next_21_num  bigint,
    next_22_num  bigint,
    next_23_num  bigint,
    next_24_num  bigint,
    -- buyer_type  string COMMENT '买家类型：only_pre_direct(只购买过前置仓直发),include_pre_direct(购买过前置仓直发),no_pre_direct(未购买过前置仓直发)',
    buyer_type      string comment '用户是否当月新激活:all、Y、N'
) COMMENT '用户复购月度报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_order_month_start_up_cohort/"
;


# 2021-01-22
dwb.dwb_vova_user_month_cohort
dwb.dwb_vova_user_month_cohort

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_order_month_cohort

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_user_month_cohort/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_user_month_cohort/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_user_month_cohort/*

hadoop distcp -m 30 -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_user_month_cohort/  s3://bigdata-offline/warehouse/dwb/dwb_vova_user_month_cohort

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_user_month_cohort/

msck repair table dwb.dwb_vova_user_month_cohort;
select * from dwb.dwb_vova_user_month_cohort limit 20;

#
hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_user_month_cohort/pt=2021-0*


###############################################################################

dwb.dwb_vova_order_month_start_up_cohort
hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_order_month_start_up_cohort/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_order_month_start_up_cohort/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_order_month_start_up_cohort/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_order_month_start_up_cohort/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_order_month_start_up_cohort/*

hadoop distcp -m 30 -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_order_month_start_up_cohort/  s3://bigdata-offline/warehouse/dwb/dwb_vova_order_month_start_up_cohort

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_order_month_start_up_cohort/

msck repair table dwb.dwb_vova_order_month_start_up_cohort;
select * from dwb.dwb_vova_order_month_start_up_cohort limit 20;


###############################################################################

dwb.dwb_vova_order_month_cohort
hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_order_month_cohort/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_order_month_cohort/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_order_month_cohort/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_order_month_cohort/*

hadoop distcp -m 30 -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_order_month_cohort/  s3://bigdata-offline/warehouse/dwb/dwb_vova_order_month_cohort

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_order_month_cohort/

msck repair table dwb.dwb_vova_order_month_cohort;
select * from dwb.dwb_vova_order_month_cohort limit 20;

#*/###############################################################

[8963]app月度留存-presto报表中用户类型调整为是否当月新激活
任务描述
app月度留存-presto 报表

原【用户类型】的选项为前置仓相关判断，现在已经不需要该业务字段。需将其选项调整为用户是否当月新激活，选项为：all、Y、N

添加新字段 is_new string comment '用户是否当月新激活:all、Y、N'
dwb.dwb_vova_user_month_cohort              app月度留存
dwb.dwb_vova_order_month_start_up_cohort    首次支付用户月度留存
dwb.dwb_vova_order_month_cohort             用户复购月度留存

alter table dwb.dwb_vova_user_month_cohort           change column buyer_type buyer_type string COMMENT'用户是否当月新激活:all、Y、N';
alter table dwb.dwb_vova_order_month_start_up_cohort change column buyer_type buyer_type string COMMENT'用户是否当月新激活:all、Y、N';
alter table dwb.dwb_vova_order_month_cohort          change column buyer_type buyer_type string COMMENT'用户是否当月新激活:all、Y、N';
















