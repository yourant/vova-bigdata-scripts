drop table dim.dim_fn_category;
CREATE TABLE IF NOT EXISTS dim.dim_fn_category
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
    four_cat_id     bigint,
    four_cat_name   string
) COMMENT '类目维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


