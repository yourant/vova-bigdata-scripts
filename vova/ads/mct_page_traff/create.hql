--商品维度汇总表
drop table ads.ads_vova_mct_page_traff_h;
CREATE TABLE IF NOT EXISTS ads.ads_vova_mct_page_traff_h
(
    mct_id                    BIGINT COMMENT '商品所属商家ID',
    first_cat_id              BIGINT COMMENT '一级品类目ID',
    page                      STRING COMMENT '页面标识',
    country                   STRING COMMENT '国家',
    expre_cnt_1h              BIGINT COMMENT '近1一小时曝光次数'
) COMMENT '商家分级流量表'
 PARTITIONED BY ( pt string,hour string)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;