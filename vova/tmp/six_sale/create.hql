CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_vova_six_sale_d
(
    mct_name     string COMMENT 'd_date',
    first_cat_name string COMMENT 'd_date',
    rec_page_code string COMMENT 'd_date',
    impre_uv bigint COMMENT 'd_date',
    impre bigint COMMENT 'd_date',
    click bigint COMMENT 'd_date',
    click_uv bigint COMMENT 'd_date',
    payed_number bigint COMMENT 'd_date',
    payed_uv bigint COMMENT 'd_date',
    gmv double COMMENT 'd_date',
    recall_impre bigint COMMENT 'd_date',
    recall_click bigint COMMENT 'd_date',
    recall_payed_number bigint COMMENT 'd_date',
    recall_payed_uv bigint COMMENT 'd_date',
    recall_gmv double COMMENT 'd_date'
) COMMENT 'dwb_vova_web_main_goods' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

