用户留存

drop table dwb.dwb_vova_user_cohort;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_user_cohort
(
    event_date          date COMMENT '启动日期',
    datasource            string,
    region_code            string,
    main_channel            string,
    is_new_user            string,
    is_new_activate            string,
    total_next_0_num                bigint,
    total_next_1_num                bigint,
    total_next_3_num                bigint,
    total_next_7_num                bigint,
    total_next_28_num                bigint,
    total_interval_1_num                bigint,
    total_interval_2_num                bigint,
    total_interval_3_num                bigint,
    total_interval_4_num                bigint,
    total_interval_5_num                bigint,
    total_interval_6_num                bigint,
    total_interval_7_num                bigint,
    ios_next_0_num                bigint,
    ios_next_1_num                bigint,
    ios_next_3_num                bigint,
    ios_next_7_num                bigint,
    ios_next_28_num                bigint,
    ios_interval_1_num                bigint,
    ios_interval_2_num                bigint,
    ios_interval_3_num                bigint,
    ios_interval_4_num                bigint,
    ios_interval_5_num                bigint,
    ios_interval_6_num                bigint,
    ios_interval_7_num                bigint,
    android_next_0_num                bigint,
    android_next_1_num                bigint,
    android_next_3_num                bigint,
    android_next_7_num                bigint,
    android_next_28_num                bigint,
    android_interval_1_num                bigint,
    android_interval_2_num                bigint,
    android_interval_3_num                bigint,
    android_interval_4_num                bigint,
    android_interval_5_num                bigint,
    android_interval_6_num                bigint,
    android_interval_7_num                bigint
) COMMENT '用户留存报表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_user_cohort/"
;

dwb.dwb_vova_user_cohort
hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_user_cohort/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_user_cohort/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_user_cohort/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_user_cohort/*

hadoop distcp -m 30 -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_user_cohort/  s3://bigdata-offline/warehouse/dwb/dwb_vova_user_cohort

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_user_cohort/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_user_cohort/

msck repair table dwb.dwb_vova_user_cohort;
select * from dwb.dwb_vova_user_cohort limit 20;
