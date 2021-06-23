drop table dwb.dwb_vova_query_translation_quality_control;
CREATE EXTERNAL TABLE dwb.dwb_vova_query_translation_quality_control
(
    element_type               string COMMENT '搜索词',
    search_cnt            int COMMENT '搜索频次',
    search_uv          int        COMMENT '搜索人数',
    rate             decimal(13,2) COMMENT '转化率',
    rate_46             decimal(13,2) COMMENT 'rp = 46（翻译）转化率',
    rate_12             decimal(13,2) COMMENT 'rp = 12（es）转化率',
    rate_32             decimal(13,2) COMMENT 'rp = 32（高频搜索词）转化率',
    rate_50             decimal(13,2) COMMENT 'rp = 50（语义识别）转化率',
    rate_36             decimal(13,2) COMMENT 'rp = 36（意图识别）转化率'
) COMMENT '搜索翻译转化率监控'
    PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
;