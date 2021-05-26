drop table dim.dim_vova_mct_op;
CREATE EXTERNAL TABLE IF NOT EXISTS dim.dim_vova_mct_op
(
    mct_op_name   string COMMENT '商品一级类目',
    first_cat_name   string COMMENT '商品一级类目'
) COMMENT '商品维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;