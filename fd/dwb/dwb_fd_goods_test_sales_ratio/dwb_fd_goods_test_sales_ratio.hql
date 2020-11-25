create table dwb.dwb_fd_goods_test_sales_ratio
(
    project_name             STRING,
    country_code             STRING,
    platform_name            STRING,
    cat_id                   BIGINT,
    cat_name                 STRING,
    finished_thread_num      BIGINT,
    success_thread_num       BIGINT,
    finished_goods_num       BIGINT,
    success_goods_num        BIGINT,
    sum_add_uv_1m            BIGINT,
    sum_detail_add_uv_1m     BIGINT,
    sum_detail_view_uv_1m    BIGINT,
    sales_num_1m             BIGINT,
    sales_amount_1m          DECIMAL(15,4),
    sales_amount_2m          DECIMAL(15,4),
    sales_amount_3m          DECIMAL(15,4),
    category_sales_amount_1m DECIMAL(15,4),
    category_sales_amount_2m DECIMAL(15,4),
    category_sales_amount_3m DECIMAL(15,4)
)
    COMMENT '月测款成功商品次n月表现'
    PARTITIONED BY (`mt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;