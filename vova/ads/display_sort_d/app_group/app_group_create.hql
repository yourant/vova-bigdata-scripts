DROP TABLE ads.ads_vova_app_group_display_sort;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_app_group_display_sort
(
    goods_id             bigint,
    region_id            bigint,
    region_standard_type string,
    gcr_rank_desc        bigint
) COMMENT 'ads_vova_app_group_display_sort' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;