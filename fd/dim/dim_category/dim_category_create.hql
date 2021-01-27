CREATE TABLE IF NOT EXISTS dim.dim_fd_category (
`cat_id` bigint COMMENT '商品品类id',
`cat_name` string COMMENT '商品品类名',
`depth` bigint COMMENT '当前叶子深度',
`first_cat_id` bigint COMMENT '商品一级类目id',
`first_cat_name` string COMMENT '商品一级类目名',
`second_cat_id` bigint COMMENT '商品二级类目id',
`second_cat_name` string COMMENT '商品二级类目名',
`third_cat_id` bigint COMMENT '商品三级类目id',
`third_cat_name` string COMMENT '商品三级类目名',
`is_leaf` bigint COMMENT '是否是叶子节点'
) COMMENT 'category维度表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;