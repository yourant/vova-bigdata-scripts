drop table dim.dim_vova_trigram_nuwa_pdd_category;
CREATE EXTERNAL TABLE IF NOT EXISTS dim.dim_vova_trigram_nuwa_pdd_category
(
    cat_id          bigint,
    cat_name        string,
    depth           bigint,
    first_cat_id    bigint,
    first_cat_name  string,
    second_cat_id   bigint,
    second_cat_name string,
    three_cat_id    bigint,
    three_cat_name  string,
    is_leaf         bigint
) COMMENT 'pdd类目维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;