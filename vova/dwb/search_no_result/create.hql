搜索无结果推荐列表页监控
需求方及需求号：张阳 ,6212
创建时间及开发人员：2020/10/16,陈凯

drop table dwb.dwb_vova_search_no_result_frequent_word;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_search_no_result_frequent_word
(
    datasource                 string   COMMENT 'vova|airyclub',
    region_code                string   comment '国家/地区',
    search_word                string   comment 'query词',
    is_brand_word              string   comment '是否brand词:all/是/否',
    search_pv                  int      comment '搜索频次',
    search_uv                  int      comment '搜索uv',
    search_no_result_pv        int      comment '搜索无结果频次',
    search_no_result_uv        int      comment '搜索无结果uv'
)COMMENT '搜索无结果高频词监控' PARTITIONED BY (pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result_frequent_word/"
;


drop table dwb.dwb_vova_search_no_result;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_search_no_result
(
    datasource                 string   COMMENT 'vova/airyclub',
    region_code                string   comment '国家/地区',
    platform                   string   comment '平台:ios/android',
    is_brand_word              string   comment '是否brand词:all/是/否',

    no_result_word_cnt         int      comment '无结果query数,搜索结果返回为空的query词去重后的个数',
    search_pv                  int      comment '搜索频次',
    search_uv                  int      comment '搜索uv',
    search_no_result_pv        int      comment '搜索无结果频次',
    search_no_result_uv        int      comment '搜索无结果uv'
)COMMENT '搜索无结果率监控' PARTITIONED BY (pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result/"
;

dwb.dwb_vova_search_no_result_frequent_word

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result_frequent_word/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result_frequent_word/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result_frequent_word/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_search_no_result_frequent_word/*

hadoop distcp -overwrite -m 20 hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_search_no_result_frequent_word/  s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result_frequent_word

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result_frequent_word/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result_frequent_word/

msck repair table dwb.dwb_vova_search_no_result_frequent_word;
select * from dwb.dwb_vova_search_no_result_frequent_word limit 20;

#
hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result_frequent_word/pt=2021-0*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_search_no_result_frequent_word/pt=2021-01-22  s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result_frequent_word/pt=2021-01-22
hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_search_no_result_frequent_word/pt=2021-01-23  s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result_frequent_word/pt=2021-01-23
hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_search_no_result_frequent_word/pt=2021-01-24  s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result_frequent_word/pt=2021-01-24

@@@@@@@@@@@@@@@@@@@@@@@

dwb.dwb_vova_search_no_result

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result/*

hadoop fs -rm -r s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result/*

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result/*

hadoop fs -du -s -h /user/hive/warehouse/rpt.db/rpt_search_no_result/*

hadoop distcp -overwrite -m 20 hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_search_no_result/  s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result

hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result/*

emrfs sync s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result/

msck repair table dwb.dwb_vova_search_no_result;
select * from dwb.dwb_vova_search_no_result limit 20;

#
hadoop fs -du -s -h s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result/pt=2021-0*

hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_search_no_result/pt=2021-01-22  s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result/pt=2021-01-22
hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_search_no_result/pt=2021-01-23  s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result/pt=2021-01-23
hadoop distcp -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/rpt.db/rpt_search_no_result/pt=2021-01-24  s3://bigdata-offline/warehouse/dwb/dwb_vova_search_no_result/pt=2021-01-24



