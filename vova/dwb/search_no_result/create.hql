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

