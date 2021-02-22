消息中心-推送报表v2



tmp.tmp_push_click
drop table if exists  dwb.dwb_vova_push_click_behavior_v2;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_push_click_behavior_v2
(
    datasource           string,
    push_date            date,
    platform             string,
    region_code          string,
    click_interval       string,
    target_type          string,
    r_tag                string,
    f_tag                string,
    m_tag                string,
    is_new               string,
    config_id            string,
    main_channel         string,
    push_click_uv        bigint,
    impressions          bigint,
    impressions_uv       bigint,
    impressions_pd       bigint,
    impressions_pd_uv    bigint,
    impressions_ex_pd    bigint,
    impressions_ex_pd_uv bigint,
    carts                bigint,
    carts_uv             bigint,
    orders               bigint,
    orders_uv            bigint,
    pays                 bigint,
    pays_uv              bigint,
    gmv                  bigint,
    try_num              bigint,
    push_num             bigint,
    success_num          bigint,
    job_rate             string COMMENT 'job_rate',
    brand_gmv            bigint,
    no_brand_gmv         bigint
) COMMENT '推送点击报表' PARTITIONED BY (pt STRING, intervals string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2/"
;

消息中心-推送报表v2
2021-01-23 历史数据迁移
dwb.dwb_vova_push_click_behavior_v2

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_push_click_behavior_v2/*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_push_click_behavior_v2/  s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2/

msck repair table dwb.dwb_vova_push_click_behavior_v2;
select * from dwb.dwb_vova_push_click_behavior_v2 limit 20;

#
hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2/pt=2021-0*

hadoop distcp -overwrite -m 3 hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_push_click_behavior_v2/pt=2021-01-18  s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2/pt=2021-01-18
hadoop distcp -overwrite -m 3 hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_push_click_behavior_v2/pt=2021-01-19  s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2/pt=2021-01-19

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_push_click_behavior_v2/pt=2021-01-20  s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2/pt=2021-01-20
hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_push_click_behavior_v2/pt=2021-01-21  s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2/pt=2021-01-21
hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_push_click_behavior_v2/pt=2021-01-22  s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2/pt=2021-01-22
hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_push_click_behavior_v2/pt=2021-01-23  s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2/pt=2021-01-23
hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_push_click_behavior_v2/pt=2021-01-24  s3://bigdata-offline/warehouse/dwb/dwb_vova_push_click_behavior_v2/pt=2021-01-24


