CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_recall_page_analysis
(
    p_type                string               comment 'page_code+list_type',
    version               string               comment '召回版本',
    expre_cnt             bigint               comment '曝光数量',
    rate                  decimal(13,2)        comment '转化率'
) COMMENT '分页面召回池曝光转化率表' PARTITIONED BY (pt String) STORED AS PARQUETFILE;