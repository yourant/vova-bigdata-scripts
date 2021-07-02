recall_pool_va

# rpt.rpt_recall_pool

CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_recall_pool(
  datasource        string,
  event_hour        string,
  platform          string,
  page_code         string,
  list_type         string,
  rp_name           string,
  is_single         string,
  test_name         string,
  goods_cnt         bigint,
  pay_uv            bigint,
  gmv               bigint,
  goods_clicks      bigint,
  goods_impressions bigint,
  recall_uv         bigint
) COMMENT '推送点击报表' PARTITIONED BY (event_date STRING)

ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_recall_pool/"
;
hadoop fs -ls s3://bigdata-offline/warehouse/dwb/dwb_vova_recall_pool/

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_recall_pool/

-- jar 包位置
aws s3 ls s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/

aws s3 cp s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar s3://vomkt-emr-rec/jar/dwb-vova-recall-pool/

aws s3 ls s3://vomkt-emr-rec/jar/dwb-vova-recall-pool/

aws s3 rm s3://vomkt-emr-rec/jar/dwb-vova-recall-pool/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar


recall_pool_va
2021-01-23 历史数据迁移
dwb.dwb_vova_recall_pool

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_recall_pool/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_recall_pool/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_recall_pool/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_recall_pool/*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_recall_pool/  s3://bigdata-offline/warehouse/dwb/dwb_vova_recall_pool

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_recall_pool/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_recall_pool/

msck repair table dwb.dwb_vova_recall_pool;
select * from dwb.dwb_vova_recall_pool limit 20;

#
hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_recall_pool/event_date=2021*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_recall_pool/event_date=2021-01-24  s3://bigdata-offline/warehouse/dwb/dwb_vova_recall_pool/event_date=2021-01-24
hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_recall_pool/event_date=2021-01-25  s3://bigdata-offline/warehouse/dwb/dwb_vova_recall_pool/event_date=2021-01-25
